import { Radio } from "lucide-react";
import { CodeBlock } from "@/components/code-block";
import { EmptyState } from "@/components/empty-state";
import { useOverview } from "@/hooks/queries";
import { curlExample } from "@/lib/requests";

/** Shown when the gateway hasn't seen any traffic: teaches the first request. */
export function NoRequests({ className }: { className?: string }) {
  const { data } = useOverview();
  return (
    <EmptyState icon={Radio} title="No requests yet" className={className}>
      <p>
        Point any OpenAI or Anthropic client at this gateway, or try it from a terminal. Requests
        show up here as they happen.
      </p>
      <CodeBlock
        className="w-full text-left"
        code={curlExample(window.location.origin, data?.auth_required ?? false)}
      />
    </EmptyState>
  );
}
