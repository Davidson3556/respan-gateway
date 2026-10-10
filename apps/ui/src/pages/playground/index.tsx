import { AudioLines, Clapperboard, ImageIcon, MessageSquare, Mic, type LucideIcon } from "lucide-react";
import type { ComponentType } from "react";
import { useSearchParams } from "react-router";
import { PageHeader } from "@/components/page-header";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "@/components/ui/tabs";
import { ChatTab } from "./chat";
import { ImageTab } from "./image";
import { SpeechTab } from "./speech";
import { TranscribeTab } from "./transcribe";
import { VideoTab } from "./video";

const tabs: { value: string; label: string; icon: LucideIcon; Panel: ComponentType }[] = [
  { value: "chat", label: "Chat", icon: MessageSquare, Panel: ChatTab },
  { value: "image", label: "Image", icon: ImageIcon, Panel: ImageTab },
  { value: "transcribe", label: "Transcribe", icon: Mic, Panel: TranscribeTab },
  { value: "speech", label: "Speech", icon: AudioLines, Panel: SpeechTab },
  { value: "video", label: "Video", icon: Clapperboard, Panel: VideoTab },
];

export function PlaygroundPage() {
  const [params, setParams] = useSearchParams();
  const requested = params.get("tab");
  const tab = tabs.some((t) => t.value === requested) ? requested! : "chat";

  return (
    <div className="grid gap-6">
      <PageHeader
        title="Playground"
        description="Try any model through the gateway. Every call here shows up under Requests."
      />
      <Tabs value={tab} onValueChange={(value) => setParams({ tab: value }, { replace: true })}>
        <TabsList className="w-full justify-start overflow-x-auto sm:w-fit">
          {tabs.map(({ value, label, icon: Icon }) => (
            <TabsTrigger key={value} value={value} className="px-3">
              <Icon /> {label}
            </TabsTrigger>
          ))}
        </TabsList>
        {/* Panels stay mounted so a conversation or a running job survives a tab switch. */}
        {tabs.map(({ value, Panel }) => (
          <TabsContent key={value} value={value} forceMount className="mt-4 data-[state=inactive]:hidden">
            <Panel />
          </TabsContent>
        ))}
      </Tabs>
    </div>
  );
}
