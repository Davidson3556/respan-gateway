/** A model to choose: its ID, and the provider that lists it, if known. */
export interface Listed {
  id: string;
  owned_by?: string;
}

/** `models` by provider, providers in the order they are first listed. */
export function byProvider<M extends Listed>(models: M[]): [string, M[]][] {
  const groups = new Map<string, M[]>();
  for (const m of models) {
    const provider = m.owned_by ?? "";
    groups.set(provider, [...(groups.get(provider) ?? []), m]);
  }
  return [...groups];
}
