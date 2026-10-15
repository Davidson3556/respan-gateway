import { useState } from "react";
import { useProviders } from "@/hooks/queries";
import { vendorIcon } from "@/lib/vendor-icons";

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

const sizes = {
  sm: { tile: "size-5 rounded-md text-[10px]", image: "size-3.5", px: 14 },
  md: { tile: "size-8 rounded-lg text-sm", image: "size-5", px: 20 },
};

/**
 * Provider `name`'s icon: its vendor's logo, bundled for common vendors (by
 * its vendor, else its base URL's domain); Respan's own; else its upstream's
 * favicon; else its initial. Its vendor and base URL are the config's unless
 * given. Inline at `sm`, a tile at `md`.
 */
export function ProviderIcon({
  name,
  baseUrl,
  vendor,
  size = "md",
}: {
  name: string;
  baseUrl?: string;
  vendor?: string | null;
  size?: keyof typeof sizes;
}) {
  const { data: providers } = useProviders();
  const provider = providers?.find((p) => p.name === name);
  const domain = providerDomain(baseUrl ?? provider?.base_url ?? "");
  const logo = vendorIcon(vendor ?? provider?.vendor, domain);
  const [failedDomain, setFailedDomain] = useState<string>();
  const respan = domain === "respan.ai" || domain?.endsWith(".respan.ai");
  const src = respan
    ? `${import.meta.env.BASE_URL}respan-logo.svg`
    : domain
      ? `https://www.google.com/s2/favicons?${new URLSearchParams({ domain, sz: "64" })}`
      : undefined;
  const { tile, image, px } = sizes[size];

  return (
    <span
      aria-hidden="true"
      className={`flex shrink-0 items-center justify-center bg-brand-soft font-semibold text-brand uppercase ${tile}`}
    >
      {logo?.mono ? (
        <span
          className={`${image} bg-foreground`}
          style={{ mask: `url("${logo.src}") center / contain no-repeat` }}
        />
      ) : logo ? (
        <img src={logo.src} alt="" width={px} height={px} className={`${image} object-contain`} />
      ) : src && failedDomain !== domain ? (
        <img
          key={domain}
          src={src}
          alt=""
          width={px}
          height={px}
          loading="lazy"
          referrerPolicy="no-referrer"
          className={`${image} object-contain ${respan ? "invert dark:invert-0" : ""}`}
          onError={() => setFailedDomain(domain)}
        />
      ) : (
        name[0]
      )}
    </span>
  );
}
