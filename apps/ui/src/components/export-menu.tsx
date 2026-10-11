import { useMutation } from "@tanstack/react-query";
import { Download } from "lucide-react";
import { toast } from "sonner";
import { Button } from "@/components/ui/button";
import {
  DropdownMenu,
  DropdownMenuContent,
  DropdownMenuItem,
  DropdownMenuTrigger,
} from "@/components/ui/dropdown-menu";
import { downloadExport, type ExportFormat } from "@/lib/export";
import { describeError } from "@/lib/query-client";
import type { RequestFilter } from "@/lib/requests";

/** Downloads the gateway's kept requests, narrowed by the filters on screen. */
export function ExportMenu({ filter }: { filter: RequestFilter }) {
  const exporting = useMutation({
    mutationFn: (format: ExportFormat) => downloadExport(format, filter),
    onError: (error) => toast.error("Couldn't export requests", { description: describeError(error) }),
  });
  return (
    <DropdownMenu>
      <DropdownMenuTrigger asChild>
        <Button variant="outline" size="sm" disabled={exporting.isPending}>
          <Download /> {exporting.isPending ? "Exporting…" : "Export"}
        </Button>
      </DropdownMenuTrigger>
      <DropdownMenuContent align="end">
        <DropdownMenuItem onSelect={() => exporting.mutate("jsonl")}>JSON Lines (.jsonl)</DropdownMenuItem>
        <DropdownMenuItem onSelect={() => exporting.mutate("csv")}>CSV (.csv)</DropdownMenuItem>
      </DropdownMenuContent>
    </DropdownMenu>
  );
}
