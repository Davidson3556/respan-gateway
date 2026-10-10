//! The native half of the `respan-gateway` Python package.
//!
//! Requests go through the gateway's router in-process (no socket); servers
//! and the command line run on one shared tokio runtime. Every call that
//! waits releases the GIL.

use std::sync::{Arc, Mutex, OnceLock};

use axum::Router;
use axum::body::Body;
use bytes::Bytes;
use http::{HeaderName, HeaderValue, Method, Request};
use http_body_util::BodyExt;
use pyo3::create_exception;
use pyo3::exceptions::{PyException, PyValueError};
use pyo3::prelude::*;
use pyo3::types::{PyBytes, PyTuple};
use respan_gateway::{App, Config};
use tokio::runtime::Runtime;
use tokio::sync::{Notify, oneshot};
use tokio::task::JoinHandle;
use tower::ServiceExt;

create_exception!(
    _native,
    ConfigError,
    PyValueError,
    "The configuration is invalid."
);
create_exception!(
    _native,
    GatewayError,
    PyException,
    "The gateway could not carry out a request or start a server."
);

/// The runtime shared by every gateway, server and the command line.
fn runtime() -> &'static Runtime {
    static RUNTIME: OnceLock<Runtime> = OnceLock::new();
    RUNTIME.get_or_init(|| {
        tokio::runtime::Builder::new_multi_thread()
            .enable_all()
            .thread_name("respan-gateway")
            .build()
            .expect("the tokio runtime starts")
    })
}

fn gateway_error(e: impl std::fmt::Display) -> PyErr {
    GatewayError::new_err(e.to_string())
}

/// A header value as the string of its bytes, one char per byte (latin-1),
/// which Python turns back into bytes.
fn latin1(bytes: &[u8]) -> String {
    bytes.iter().map(|&b| char::from(b)).collect()
}

fn unlatin1(text: &str) -> Result<Vec<u8>, PyErr> {
    text.chars()
        .map(|c| u8::try_from(c).map_err(|_| gateway_error("header text is not latin-1")))
        .collect()
}

/// A configured gateway.
#[pyclass(frozen, module = "respan_gateway._native")]
#[derive(Debug)]
struct Native {
    app: Arc<App>,
    router: Router,
}

impl Native {
    fn new(config: Result<Config, respan_gateway::Error>) -> PyResult<Self> {
        let config_error = |e: respan_gateway::Error| ConfigError::new_err(e.to_string());
        // Providers may need the runtime to set up their clients.
        let _entered = runtime().enter();
        let app = App::new(config.map_err(config_error)?).map_err(config_error)?;
        Ok(Self {
            router: app.router(),
            app: Arc::new(app),
        })
    }
}

#[pymethods]
impl Native {
    /// A gateway from YAML (JSON is YAML).
    #[staticmethod]
    fn from_yaml(text: &str) -> PyResult<Self> {
        Self::new(Config::parse(text))
    }

    /// A gateway from the YAML file at `path`.
    #[staticmethod]
    fn from_file(path: &str) -> PyResult<Self> {
        Self::new(Config::load(path))
    }

    /// A gateway from the vendors the environment has keys for.
    #[staticmethod]
    fn from_env() -> PyResult<Self> {
        Self::new(Config::from_env())
    }

    /// Sends one request through the router and returns once its head is
    /// ready; the body is then pulled from the returned [`Reply`].
    fn send(
        &self,
        py: Python<'_>,
        method: &str,
        target: &str,
        headers: Vec<(String, String)>,
        body: &[u8],
    ) -> PyResult<Reply> {
        let mut request = Request::builder()
            .method(Method::from_bytes(method.as_bytes()).map_err(gateway_error)?)
            .uri(target);
        for (name, value) in &headers {
            let name = HeaderName::from_bytes(name.as_bytes()).map_err(gateway_error)?;
            let value = HeaderValue::from_bytes(&unlatin1(value)?).map_err(gateway_error)?;
            request = request.header(name, value);
        }
        let request = request
            .body(Body::from(Bytes::copy_from_slice(body)))
            .map_err(gateway_error)?;
        let router = self.router.clone();
        let response = py.detach(|| {
            runtime().block_on(async { router.oneshot(request).await.map_err(gateway_error) })
        })?;
        let (head, body) = response.into_parts();
        Ok(Reply {
            status: head.status.as_u16(),
            headers: head
                .headers
                .iter()
                .map(|(n, v)| (n.as_str().to_owned(), latin1(v.as_bytes())))
                .collect(),
            body: tokio::sync::Mutex::new(body),
            closed: Notify::new(),
        })
    }

    /// Starts serving on `host:port` (port 0 picks a free one).
    fn serve(&self, py: Python<'_>, host: &str, port: u16) -> PyResult<Server> {
        let app = self.app.clone();
        let host = host.to_owned();
        py.detach(|| {
            runtime().block_on(async move {
                let listener = tokio::net::TcpListener::bind((host.as_str(), port))
                    .await
                    .map_err(|e| gateway_error(format!("listen on {host}:{port}: {e}")))?;
                let addr = listener.local_addr().map_err(gateway_error)?;
                let banner = app.banner(addr);
                let (stop, stopped) = oneshot::channel::<()>();
                let task = tokio::spawn(async move {
                    app.serve(listener, async {
                        let _ = stopped.await;
                    })
                    .await
                });
                Ok(Server {
                    url: format!("http://{addr}"),
                    banner,
                    stop: Mutex::new(Some(stop)),
                    task: Mutex::new(Some(task)),
                })
            })
        })
    }
}

/// A response whose head has arrived and whose body is pulled chunk by chunk.
#[pyclass(frozen, module = "respan_gateway._native")]
#[derive(Debug)]
struct Reply {
    #[pyo3(get)]
    status: u16,
    /// Header names and latin-1 values, in order.
    #[pyo3(get)]
    headers: Vec<(String, String)>,
    body: tokio::sync::Mutex<Body>,
    closed: Notify,
}

#[pymethods]
impl Reply {
    /// The next chunk of the body, or `None` at its end, blocking (GIL
    /// released) until one arrives. Returns `None` once closed, even from
    /// another thread.
    fn next_chunk<'py>(&self, py: Python<'py>) -> PyResult<Option<Bound<'py, PyBytes>>> {
        let chunk = py.detach(|| {
            runtime().block_on(async {
                let mut body = self.body.lock().await;
                loop {
                    let frame = tokio::select! {
                        biased;
                        () = self.closed.notified() => return Ok(None),
                        frame = body.frame() => frame,
                    };
                    match frame {
                        None => return Ok(None),
                        Some(Err(e)) => return Err(gateway_error(e)),
                        // Trailers carry nothing a client reads.
                        Some(Ok(frame)) => {
                            if let Ok(data) = frame.into_data()
                                && !data.is_empty()
                            {
                                return Ok(Some(data));
                            }
                        }
                    }
                }
            })
        })?;
        Ok(chunk.map(|data| PyBytes::new(py, &data)))
    }

    /// Abandons the body; a blocked `next_chunk` returns `None`.
    fn close(&self) {
        // A permit, so a close before the next wait is not lost.
        self.closed.notify_one();
    }
}

/// A gateway listening on a real socket in the background.
#[pyclass(frozen, module = "respan_gateway._native")]
#[derive(Debug)]
struct Server {
    #[pyo3(get)]
    url: String,
    /// What the binary prints on start: providers, endpoints, the UI.
    #[pyo3(get)]
    banner: String,
    stop: Mutex<Option<oneshot::Sender<()>>>,
    task: Mutex<Option<JoinHandle<std::io::Result<()>>>>,
}

impl Server {
    fn signal(&self) {
        if let Some(stop) = self.stop.lock().ok().and_then(|mut s| s.take()) {
            let _ = stop.send(());
        }
    }
}

impl Drop for Server {
    fn drop(&mut self) {
        self.signal();
    }
}

#[pymethods]
impl Server {
    /// Stops accepting, lets open responses finish (up to 30 s) and returns.
    fn stop(&self, py: Python<'_>) -> PyResult<()> {
        self.signal();
        let task = self.task.lock().ok().and_then(|mut t| t.take());
        let Some(task) = task else { return Ok(()) };
        py.detach(|| runtime().block_on(task))
            .map_err(gateway_error)?
            .map_err(gateway_error)
    }

    fn __enter__(slf: Py<Self>) -> Py<Self> {
        slf
    }

    #[pyo3(signature = (*_exc))]
    fn __exit__(&self, py: Python<'_>, _exc: &Bound<'_, PyTuple>) -> PyResult<()> {
        self.stop(py)
    }
}

/// Runs the `respan-gateway` command line on `args`, the program name
/// first, and returns its exit status.
#[pyfunction]
fn run_cli(py: Python<'_>, args: Vec<String>) -> i32 {
    py.detach(|| i32::from(runtime().block_on(respan_gateway::run(args))))
}

#[pymodule]
fn _native(m: &Bound<'_, PyModule>) -> PyResult<()> {
    m.add_class::<Native>()?;
    m.add_class::<Reply>()?;
    m.add_class::<Server>()?;
    m.add_function(wrap_pyfunction!(run_cli, m)?)?;
    m.add("ConfigError", m.py().get_type::<ConfigError>())?;
    m.add("GatewayError", m.py().get_type::<GatewayError>())?;
    m.add("__version__", env!("CARGO_PKG_VERSION"))?;
    Ok(())
}
