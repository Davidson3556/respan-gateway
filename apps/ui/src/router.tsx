import { createBrowserRouter } from "react-router";
import { AppLayout } from "@/components/layout/app-layout";
import { NotFoundPage } from "@/pages/not-found";
import { OverviewPage } from "@/pages/overview";
import { ProvidersPage } from "@/pages/providers";
import { RequestDetailSheet } from "@/pages/request-detail";
import { RequestsPage } from "@/pages/requests";
import { RoutesPage } from "@/pages/routes";
import { SettingsPage } from "@/pages/settings";

export const routes = [
  {
    element: <AppLayout />,
    children: [
      { index: true, element: <OverviewPage /> },
      {
        path: "requests",
        element: <RequestsPage />,
        children: [{ path: ":id", element: <RequestDetailSheet /> }],
      },
      {
        path: "playground",
        lazy: () => import("@/pages/playground").then((m) => ({ Component: m.PlaygroundPage })),
      },
      { path: "providers", element: <ProvidersPage /> },
      { path: "routes", element: <RoutesPage /> },
      { path: "settings", element: <SettingsPage /> },
      { path: "*", element: <NotFoundPage /> },
    ],
  },
];

export function createRouter() {
  return createBrowserRouter(routes, { basename: "/ui" });
}
