/** Pure functions for validating broadcast recipients and send failures. */
export const MAX_BROADCAST_RECIPIENTS = 500;
export const MAX_SEND_ATTEMPTS = 4;

export function normalizeRecipient(value: string): string {
    if (typeof value !== "string") throw new Error("Recipient must be a string");
    let v = value.trim();
    if (!v) throw new Error("Recipient cannot be empty");

    // Preserve explicitly addressed groups and linked IDs. Validate their syntax.
    if (/^[0-9-]+@g\.us$/.test(v) || /^\d+@lid$/.test(v)) return v;
    if (v.endsWith("@s.whatsapp.net")) v = v.slice(0, -"@s.whatsapp.net".length);
    if (v.includes("@")) throw new Error(`Unsupported recipient identifier: ${value}`);

    // Accept common number formatting, but never guess arbitrary country codes.
    let digits = v.replace(/[\s()+-]/g, "");
    if (!/^\d+$/.test(digits)) throw new Error(`Invalid phone number: ${value}`);
    // India-only default for exactly ten digit domestic mobile numbers.
    if (/^[6-9]\d{9}$/.test(digits)) digits = `91${digits}`;
    if (!/^\d{8,15}$/.test(digits)) throw new Error(`Phone must contain 8–15 digits with country code: ${value}`);
    return `${digits}@s.whatsapp.net`;
}

export function normalizeRecipients(values: string[]): string[] {
    if (!Array.isArray(values) || values.length < 1 || values.length > MAX_BROADCAST_RECIPIENTS) {
        throw new Error(`Select between 1 and ${MAX_BROADCAST_RECIPIENTS} recipients`);
    }
    const items = [...new Set(values.map(normalizeRecipient))];
    if (!items.length) throw new Error("At least one valid recipient is required");
    return items;
}

export function isTemporarySendError(err: unknown): boolean {
    const msg = err instanceof Error ? err.message : String(err ?? "");
    return /not connected|not ready|connection|disconnect|closed|timeout|timed out|econnreset|econnrefused|etimedout|network|restartrequired|503|502|429|rate limit|socket/i.test(msg);
}

export function errorText(err: unknown): string {
    const str = err instanceof Error ? err.message : String(err ?? "Unexpected send failure");
    return str.slice(0, 400);
}

export function retryDelayMs(attempt: number): number {
    return Math.min(15000, 1500 * Math.pow(2, Math.max(0, attempt - 1)));
}
