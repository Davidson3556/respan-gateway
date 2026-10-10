import {
  Activity,
  FlaskConical,
  LayoutDashboard,
  Route,
  Server,
  Settings,
  type LucideIcon,
} from "lucide-react";
import { NavLink, useLocation } from "react-router";
import {
  Sidebar,
  SidebarContent,
  SidebarFooter,
  SidebarGroup,
  SidebarGroupContent,
  SidebarGroupLabel,
  SidebarHeader,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  SidebarRail,
  useSidebar,
} from "@/components/ui/sidebar";
import { useOverview } from "@/hooks/queries";

interface NavItem {
  to: string;
  label: string;
  icon: LucideIcon;
}

const monitor: NavItem[] = [
  { to: "/", label: "Overview", icon: LayoutDashboard },
  { to: "/requests", label: "Requests", icon: Activity },
];

const build: NavItem[] = [
  { to: "/playground", label: "Playground", icon: FlaskConical },
  { to: "/providers", label: "Providers", icon: Server },
  { to: "/routes", label: "Routes", icon: Route },
];

const footer: NavItem[] = [{ to: "/settings", label: "Settings", icon: Settings }];

function NavMenu({ items }: { items: NavItem[] }) {
  const { pathname } = useLocation();
  const { isMobile, setOpenMobile } = useSidebar();
  return (
    <SidebarMenu>
      {items.map(({ to, label, icon: Icon }) => (
        <SidebarMenuItem key={to}>
          <SidebarMenuButton
            asChild
            isActive={to === "/" ? pathname === "/" : pathname.startsWith(to)}
            tooltip={label}
          >
            <NavLink to={to} end={to === "/"} onClick={() => isMobile && setOpenMobile(false)}>
              <Icon />
              <span>{label}</span>
            </NavLink>
          </SidebarMenuButton>
        </SidebarMenuItem>
      ))}
    </SidebarMenu>
  );
}

function NavGroup({ label, items }: { label: string; items: NavItem[] }) {
  return (
    <SidebarGroup>
      <SidebarGroupLabel>{label}</SidebarGroupLabel>
      <SidebarGroupContent>
        <NavMenu items={items} />
      </SidebarGroupContent>
    </SidebarGroup>
  );
}

function Brand() {
  const { data } = useOverview();
  return (
    <div className="flex items-center gap-2.5 px-1 py-1.5">
      <div className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-brand text-white shadow-sm">
        <svg viewBox="0 0 32 32" className="size-5" aria-hidden="true">
          <path
            d="M6 16h6l3-6 4 12 3-6h4"
            fill="none"
            stroke="currentColor"
            strokeWidth="2.6"
            strokeLinecap="round"
            strokeLinejoin="round"
          />
        </svg>
      </div>
      <div className="grid min-w-0 leading-tight group-data-[collapsible=icon]:hidden">
        <span className="truncate text-sm font-semibold">respan gateway</span>
        <span className="truncate font-mono text-[11px] text-muted-foreground">
          {data ? `v${data.version} · ${data.listen}` : "connecting…"}
        </span>
      </div>
    </div>
  );
}

export function AppSidebar() {
  return (
    <Sidebar collapsible="icon" variant="inset">
      <SidebarHeader>
        <Brand />
      </SidebarHeader>
      <SidebarContent>
        <NavGroup label="Monitor" items={monitor} />
        <NavGroup label="Build" items={build} />
      </SidebarContent>
      <SidebarFooter>
        <NavMenu items={footer} />
      </SidebarFooter>
      <SidebarRail />
    </Sidebar>
  );
}
