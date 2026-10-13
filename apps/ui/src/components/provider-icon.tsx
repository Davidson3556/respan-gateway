import { useState } from "react";

/** Use the upstream's identity, even when it speaks another vendor's API. */
function providerDomain(baseUrl: string): string | undefined {
  try {
    const url = new URL(baseUrl);
    if (url.protocol !== "https:" && url.protocol !== "http:") return;
    return url.hostname.replace(/^api\./i, "");
  } catch {
    return;
  }
}

export function ProviderIcon({ name, baseUrl }: { name: string; baseUrl: string }) {
  const domain = providerDomain(baseUrl);
  const [failedDomain, setFailedDomain] = useState<string>();
  const respan = domain === "respan.ai" || domain?.endsWith(".respan.ai");
  const src = respan
    ? `${import.meta.env.BASE_URL}respan-logo.svg`
    : domain
      ? `https://www.google.com/s2/favicons?${new URLSearchParams({ domain, sz: "64" })}`
      : undefined;

  return (
    <span
      aria-hidden="true"
      className="flex size-8 shrink-0 items-center justify-center rounded-lg bg-brand-soft text-sm font-semibold text-brand uppercase"
    >
      {src && failedDomain !== domain ? (
        <img
          key={domain}
          src={src}
          alt=""
          width={20}
          height={20}
          loading="lazy"
          referrerPolicy="no-referrer"
          className={`size-5 object-contain ${respan ? "invert dark:invert-0" : ""}`}
          onError={() => setFailedDomain(domain)}
        />
      ) : (
        name[0]
      )}
    </span>
  );
}
