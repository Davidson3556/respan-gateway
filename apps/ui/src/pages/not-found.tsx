import { Compass } from "lucide-react";
import { Link, useLocation } from "react-router";
import { EmptyState } from "@/components/empty-state";
import { Button } from "@/components/ui/button";

export function NotFoundPage() {
  const { pathname } = useLocation();
  return (
    <EmptyState icon={Compass} title="Page not found" className="mt-12">
      <p>
        Nothing lives at <code className="text-foreground">/ui{pathname}</code>.
      </p>
      <Button asChild size="sm">
        <Link to="/">Back to overview</Link>
      </Button>
    </EmptyState>
  );
}
