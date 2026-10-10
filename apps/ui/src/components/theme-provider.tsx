import { useEffect, useMemo, useState, useSyncExternalStore, type ReactNode } from "react";
import { THEME_STORAGE, ThemeContext, type Theme } from "@/hooks/use-theme";

const query = "(prefers-color-scheme: dark)";

function subscribeSystem(onChange: () => void) {
  const mql = window.matchMedia(query);
  mql.addEventListener("change", onChange);
  return () => mql.removeEventListener("change", onChange);
}

function readStored(): Theme {
  try {
    const t = localStorage.getItem(THEME_STORAGE);
    return t === "light" || t === "dark" ? t : "system";
  } catch {
    return "system";
  }
}

export function ThemeProvider({ children }: { children: ReactNode }) {
  const [theme, setThemeState] = useState<Theme>(readStored);
  const systemDark = useSyncExternalStore(
    subscribeSystem,
    () => window.matchMedia(query).matches,
    () => false,
  );
  const resolved = theme === "system" ? (systemDark ? "dark" : "light") : theme;

  useEffect(() => {
    document.documentElement.classList.toggle("dark", resolved === "dark");
  }, [resolved]);

  const value = useMemo(
    () => ({
      theme,
      resolved,
      setTheme: (next: Theme) => {
        try {
          localStorage.setItem(THEME_STORAGE, next);
        } catch {
          // Not persisted; still applied for this session.
        }
        setThemeState(next);
      },
    }),
    [theme, resolved],
  );

  return <ThemeContext.Provider value={value}>{children}</ThemeContext.Provider>;
}
