import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { MemoryRouter, useLocation } from "react-router";
import { describe, expect, it, vi } from "vitest";
import { ThemeProvider } from "@/components/theme-provider";
import { CommandPalette } from "./command-palette";

function Where() {
  return <output aria-label="location">{useLocation().pathname}</output>;
}

function renderPalette() {
  vi.stubGlobal(
    "fetch",
    vi.fn(async (input: RequestInfo | URL) =>
      new Response(String(input).includes("/requests") ? '{"requests":[]}' : '{"providers":[]}'),
    ),
  );
  return render(
    <QueryClientProvider client={new QueryClient({ defaultOptions: { queries: { retry: false } } })}>
      <ThemeProvider>
        <MemoryRouter>
          <CommandPalette />
          <Where />
        </MemoryRouter>
      </ThemeProvider>
    </QueryClientProvider>,
  );
}

describe("CommandPalette", () => {
  it("opens with ctrl+K and jumps to a page", async () => {
    const user = userEvent.setup();
    renderPalette();
    await user.keyboard("{Control>}k{/Control}");
    await user.type(await screen.findByPlaceholderText(/jump to/i), "prov");
    await user.keyboard("{Enter}");
    expect(screen.getByLabelText("location")).toHaveTextContent("/providers");
    expect(screen.queryByPlaceholderText(/jump to/i)).not.toBeInTheDocument();
  });
});
