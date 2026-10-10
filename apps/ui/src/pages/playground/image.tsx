import { useMutation } from "@tanstack/react-query";
import { Download, ImageIcon } from "lucide-react";
import { useState } from "react";
import { EmptyState } from "@/components/empty-state";
import { Button } from "@/components/ui/button";
import { Label } from "@/components/ui/label";
import {
  Select,
  SelectContent,
  SelectItem,
  SelectTrigger,
  SelectValue,
} from "@/components/ui/select";
import { Skeleton } from "@/components/ui/skeleton";
import { Textarea } from "@/components/ui/textarea";
import { useModelChoice } from "@/hooks/use-model-choice";
import { postJson } from "@/lib/api";
import { MediaLayout } from "./media-form";
import { ModelPicker } from "./model-picker";
import { RequestError } from "./request-error";

interface ImagesResponse {
  data: { b64_json?: string; url?: string; revised_prompt?: string }[];
}

const sizes = ["1024x1024", "1536x1024", "1024x1536"];

export function ImageTab() {
  const choice = useModelChoice("image", "gpt-image-1");
  const [prompt, setPrompt] = useState("");
  const [size, setSize] = useState(sizes[0]);
  const [n, setN] = useState("1");
  const generate = useMutation({
    mutationFn: () =>
      postJson<ImagesResponse>("/v1/images/generations", {
        model: choice.model,
        prompt: prompt.trim(),
        size,
        n: Number(n),
      }),
  });

  const images = (generate.data?.data ?? []).map((img) => ({
    src: img.b64_json ? `data:image/png;base64,${img.b64_json}` : (img.url ?? ""),
    revised: img.revised_prompt,
  }));

  return (
    <MediaLayout
      onSubmit={() => generate.mutate()}
      pending={generate.isPending}
      disabled={!prompt.trim()}
      submitLabel={generate.isPending ? "Generating…" : "Generate"}
      fields={
        <>
          <div className="grid gap-2">
            <Label htmlFor="image-prompt">Prompt</Label>
            <Textarea
              id="image-prompt"
              value={prompt}
              onChange={(e) => setPrompt(e.target.value)}
              placeholder="A lighthouse at dusk, painted in soft watercolor"
              className="min-h-28"
            />
          </div>
          <ModelPicker id="image-model" choice={choice} />
          <div className="grid grid-cols-2 gap-3">
            <div className="grid gap-2">
              <Label htmlFor="image-size">Size</Label>
              <Select value={size} onValueChange={setSize}>
                <SelectTrigger id="image-size" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {sizes.map((s) => (
                    <SelectItem key={s} value={s}>
                      {s}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
            <div className="grid gap-2">
              <Label htmlFor="image-n">Images</Label>
              <Select value={n} onValueChange={setN}>
                <SelectTrigger id="image-n" className="w-full">
                  <SelectValue />
                </SelectTrigger>
                <SelectContent>
                  {["1", "2", "3", "4"].map((v) => (
                    <SelectItem key={v} value={v}>
                      {v}
                    </SelectItem>
                  ))}
                </SelectContent>
              </Select>
            </div>
          </div>
        </>
      }
    >
      {generate.isError ? (
        <RequestError error={generate.error} what="images" />
      ) : generate.isPending ? (
        <div className="grid gap-4 sm:grid-cols-2">
          {Array.from({ length: Number(n) }, (_, i) => (
            <Skeleton key={i} className="aspect-square rounded-xl" />
          ))}
        </div>
      ) : images.length ? (
        <div className="grid gap-4 sm:grid-cols-2">
          {images.map((img, i) => (
            <figure key={i} className="group relative overflow-hidden rounded-xl border bg-muted/40">
              <img src={img.src} alt={img.revised ?? prompt} className="aspect-square w-full object-cover" />
              <Button
                asChild
                size="icon"
                variant="secondary"
                className="absolute top-2 right-2 size-8 opacity-0 shadow-sm transition-opacity group-hover:opacity-100 focus-visible:opacity-100"
              >
                <a href={img.src} download={`image-${i + 1}.png`} aria-label={`Download image ${i + 1}`}>
                  <Download />
                </a>
              </Button>
              {img.revised && (
                <figcaption className="border-t p-3 text-xs text-muted-foreground">
                  {img.revised}
                </figcaption>
              )}
            </figure>
          ))}
        </div>
      ) : (
        <EmptyState icon={ImageIcon} title="Generate an image">
          <p>
            Sent to <code>/v1/images/generations</code>; results appear here, ready to download.
          </p>
        </EmptyState>
      )}
    </MediaLayout>
  );
}
