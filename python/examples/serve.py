"""Serve the gateway and call it with a plain HTTP client.

    OPENAI_API_KEY=sk-... python examples/serve.py
"""

import httpx

import respan_gateway as rg


def main(gw: rg.Gateway) -> int:
    with gw.serve(port=0) as server:  # a real socket, in the background
        print("serving at", server.url)
        reply = httpx.post(
            server.url + "/v1/chat/completions",
            json={"model": "gpt-4o-mini", "messages": [{"role": "user", "content": "Say hi."}]},
            timeout=60,
        )
        print(reply.status_code, reply.json()["choices"][0]["message"]["content"])
        return reply.status_code


if __name__ == "__main__":
    with rg.Gateway() as gw:
        main(gw)
