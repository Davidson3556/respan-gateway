import { useState } from "react";
import type { Kind, Model } from "@/lib/api";
import { useModels } from "@/hooks/queries";

export interface ModelChoice {
  /** The model to send: the user's pick, else the first listed, else `fallback`. */
  model: string;
  /** What the user typed or picked, possibly empty. */
  chosen: string;
  setModel: (model: string) => void;
  models: Model[] | undefined;
  isPending: boolean;
  fallback: string;
}

export function useModelChoice(kind: Kind, fallback: string): ModelChoice {
  const { data: models, isPending } = useModels(kind);
  const [chosen, setModel] = useState("");
  const model = chosen.trim() || models?.[0]?.id || fallback;
  return { model, chosen, setModel, models, isPending, fallback };
}
