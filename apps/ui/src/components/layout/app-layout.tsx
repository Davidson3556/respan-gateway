import { Outlet } from "react-router";
import { LiveProvider } from "@/components/live-provider";
import { SidebarInset, SidebarProvider } from "@/components/ui/sidebar";
import { AppSidebar } from "./app-sidebar";
import { KeyDialog } from "./key-dialog";
import { SiteHeader } from "./site-header";

// The shadcn sidebar remembers its state in this cookie.
const sidebarOpen = !document.cookie.includes("sidebar_state=false");

export function AppLayout() {
  return (
    <LiveProvider>
      <SidebarProvider defaultOpen={sidebarOpen}>
        <AppSidebar />
        <SidebarInset className="min-w-0">
          <SiteHeader />
          <main className="mx-auto w-full max-w-7xl flex-1 px-4 py-6 sm:px-6 lg:px-8 lg:py-8">
            <Outlet />
          </main>
        </SidebarInset>
        <KeyDialog />
      </SidebarProvider>
    </LiveProvider>
  );
}
