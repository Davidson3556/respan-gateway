import { useEffect, useState } from "react";

/** An object URL for a blob, revoked when the blob changes or on unmount. */
export function useObjectUrl(blob: Blob | null | undefined): string | null {
  const [made, setMade] = useState<{ blob: Blob; url: string } | null>(null);
  // Created and revoked in the same effect, so a StrictMode re-run never
  // revokes a URL that is still displayed.
  useEffect(() => {
    if (!blob) return;
    const url = URL.createObjectURL(blob);
    // Publishing the resource this effect just created is the point of the effect.
    // eslint-disable-next-line react-hooks/set-state-in-effect
    setMade({ blob, url });
    return () => URL.revokeObjectURL(url);
  }, [blob]);
  return blob && made?.blob === blob ? made.url : null;
}
