/**
 * The backend's `apiName` already carries the direction
 * (`ProduceRequest`, `FetchResponse`) so the filter DSL matches what the
 * list shows. Rows that style the suffix split it off here instead of
 * appending their own.
 */
export function splitApiName(apiName: string): { base: string; suffix: string } {
  const m = /^(.+?)(Request|Response)$/.exec(apiName);
  return m?.[1] !== undefined && m[2] !== undefined
    ? { base: m[1], suffix: m[2] }
    : { base: apiName, suffix: "" };
}
