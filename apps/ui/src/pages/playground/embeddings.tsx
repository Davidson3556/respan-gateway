import { useMutation } from "@tanstack/react-query";
import { Sigma } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/empty-state";
import { Card } from "@/components/ui/card";
import { Label } from "@/components/ui/label";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { useModelChoice } from "@/hooks/use-model-choice";
import { postJson } from "@/lib/api";
import { cosineSimilarity } from "@/lib/embeddings";
import { SimilarityMeter, VectorStrip } from "./embedding-visuals";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";

interface EmbeddingsResponse {
  data: { index: number; embedding: number[] }[];
}

const SHOWN = 8;

export function EmbeddingsTab() {
  const choice = useModelChoice("embedding", "text-embedding-3-small");
  const [a, setA] = useState("");
  const [b, setB] = useState("");
  const embed = useMutation({
    mutationFn: async () => {
      const res = await postJson<EmbeddingsResponse>("/v1/embeddings", {
        model: choice.model,
        input: [a.trim(), b.trim()],
        encoding_format: "float",
      });
      return [...res.data].sort((x, y) => x.index - y.index).map((d) => d.embedding);
    },
  });

  const [first, second] = embed.data ?? [];
  const texts = [a.trim(), b.trim()];
  const score = first && second ? cosineSimilarity(first, second) : 0;

  return (
    <MediaLayout
      onSubmit={() => embed.mutate()}
      pending={embed.isPending}
      disabled={!a.trim() || !b.trim()}
      submitLabel={embed.isPending ? "Embedding…" : "Embed"}
      fields={
        <>
          <div className="grid gap-2">
            <Label htmlFor="embedding-a">Text A</Label>
            <Textarea
              id="embedding-a"
              value={a}
              onChange={(e) => setA(e.target.value)}
              placeholder="The cat sat on the mat."
              className="min-h-20"
            />
          </div>
          <div className="grid gap-2">
            <Label htmlFor="embedding-b">Text B</Label>
            <Textarea
              id="embedding-b"
              value={b}
              onChange={(e) => setB(e.target.value)}
              placeholder="A kitten rested on the rug."
              className="min-h-20"
            />
          </div>
          <ModelPicker id="embedding-model" choice={choice} />
        </>
      }
    >
      {embed.isError ? (
        <RequestError error={embed.error} what="embeddings" />
      ) : embed.isPending ? (
        <Skeleton className="h-40 rounded-xl" />
      ) : first && second ? (
        <div className="grid gap-4">
          <Card className="gap-4 p-6">
            <div>
              <p className="text-sm text-muted-foreground">Cosine similarity</p>
              <p className="font-mono text-3xl font-semibold tabular-nums">{score.toFixed(4)}</p>
            </div>
            <SimilarityMeter score={score} />
          </Card>
          <div className="grid gap-4 sm:grid-cols-2">
            {[first, second].map((vector, i) => (
              <Card key={i} className="gap-2 p-4">
                <p className="text-sm font-medium">Text {i === 0 ? "A" : "B"}</p>
                <p className="truncate text-xs text-muted-foreground">{texts[i]}</p>
                <p className="text-xs text-muted-foreground">{vector.length} dimensions</p>
                <VectorStrip vector={vector} />
                <p className="font-mono text-xs break-words">
                  [{vector.slice(0, SHOWN).map((v) => v.toFixed(4)).join(", ")}
                  {vector.length > SHOWN ? ", …" : ""}]
                </p>
              </Card>
            ))}
          </div>
        </div>
      ) : (
        <EmptyState icon={Sigma} title="Compare two texts">
          <p>
            Sent to <code>/v1/embeddings</code>; each text's vector and how alike the two are appear here.
          </p>
        </EmptyState>
      )}
    </MediaLayout>
  );
}
