import { Monitor, Moon, Sun, type LucideIcon } from "lucide-react";
import { createContext, useContext } from "react";

export type Theme = "light" | "dark" | "system";
export const THEME_STORAGE = "respan.theme";

export const themeOptions: { value: Theme; label: string; icon: LucideIcon }[] = [
  { value: "light", label: "Light", icon: Sun },
  { value: "dark", label: "Dark", icon: Moon },
  { value: "system", label: "System", icon: Monitor },
];


export interface ThemeState {
  theme: Theme;
  /** The theme actually shown, with "system" resolved. */
  resolved: "light" | "dark";
  setTheme: (theme: Theme) => void;
}

export const ThemeContext = createContext<ThemeState | null>(null);

export function useTheme(): ThemeState {
  const state = useContext(ThemeContext);
  if (!state) throw new Error("useTheme must be used inside <ThemeProvider>");
  return state;
}
