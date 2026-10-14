import {
  Activity,
  FileCode,
  Blocks,
  FlaskConical,
  LayoutDashboard,
  Route,
  Server,
  Settings,
  type LucideIcon,
} from "lucide-react";

export interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
}

export const monitor: NavItem[] = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/requests", label: "Requests", icon: Activity },
];

export const build: NavItem[] = [
  { to: "/playground", label: "Playground", icon: FlaskConical },
  { to: "/providers", label: "Providers", icon: Server },
  { to: "/mcp", label: "MCP", icon: Blocks },
  { to: "/routes", label: "Routes", icon: Route },
  { to: "/config", label: "Config", icon: FileCode },
];

export const settings: NavItem = { to: "/settings", label: "Settings", icon: Settings };
