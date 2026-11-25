// Pure receipt logic, deliberately independent of socket or database.
export type ReceiptKind = 'SENT' | 'DELIVERED' | 'READ' | 'FAILED';
export function statusKind(status: unknown, e: Record<string, number>): ReceiptKind | undefined {
    if (typeof status !== 'number') return undefined;
    if (status === e.SERVER_ACK) return 'SENT';
    if (status === e.DELIVERY_ACK) return 'DELIVERED';
    if (status === e.READ || status === e.PLAYED) return 'READ';
    if (status === e.ERROR) return 'FAILED';
    return undefined;
}
export function directJid(value: unknown): string | undefined {
    if (typeof value !== 'string') return undefined;
    const match = /^(\d{5,20})(?::\d+)?@(s\.whatsapp\.net|c\.us)$/.exec(value);
    return match ? `${match[1]}@s.whatsapp.net` : undefined;
}
export function validKey(value: unknown): value is string {
    return typeof value === 'string' && /^[A-Za-z0-9_-]{1,128}$/.test(value);
}
