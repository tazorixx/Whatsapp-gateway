import { prisma } from '@/lib/prisma';
import { proto, type WASocket } from '@whiskeysockets/baileys';
import { resolveToPhoneJid } from '@/lib/jid-utils';
import { directJid, statusKind, validKey, type ReceiptKind } from './tracking-v16-logic';

// Isolated, additive receipt ledger: no change to existing Prisma models or data.
// Store first observation time, NOT an invented historical delivery timestamp.
let tableReady: Promise<unknown> | undefined;
export async function ensureV16Table() {
    if (!tableReady) {
        tableReady = prisma.$executeRaw`CREATE TABLE IF NOT EXISTS tazorixx_v16_receipts (
            session_id VARCHAR(191) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            key_id VARCHAR(128) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            jid VARCHAR(64) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            kind VARCHAR(12) CHARACTER SET ascii COLLATE ascii_bin NOT NULL,
            observed_ms BIGINT NOT NULL,
            PRIMARY KEY(session_id,key_id,jid,kind)
        ) ENGINE=InnoDB`.catch(err => { tableReady = undefined; throw err; });
    }
    await tableReady;
}
export const v16Status = (status: unknown) => statusKind(status, proto.WebMessageInfo.Status as unknown as Record<string, number>);
export async function canonicalV16Jid(jid: unknown, dbSessionId: string, alt?: string | null) {
    if (typeof jid !== 'string') return undefined;
    const direct = directJid(jid);
    if (direct) return direct;
    if (!jid.endsWith('@lid')) return undefined;
    return directJid(await resolveToPhoneJid(jid, dbSessionId, alt));
}
async function record(dbSessionId: string, key: any, kind: ReceiptKind | undefined, receiptUser?: string | null) {
    if (!kind || key?.fromMe !== true || !validKey(key?.id)) return;
    const jid = await canonicalV16Jid(key.remoteJid, dbSessionId, key.remoteJidAlt);
    if (!jid) return; // No group, status, newsletter, or unresolved LID receipts.
    if (receiptUser && await canonicalV16Jid(receiptUser, dbSessionId) !== jid) return;
    await ensureV16Table();
    const now = Date.now();
    await prisma.$executeRaw`INSERT INTO tazorixx_v16_receipts (session_id,key_id,jid,kind,observed_ms)
      VALUES (${dbSessionId},${key.id},${jid},${kind},${now})
      ON DUPLICATE KEY UPDATE observed_ms=LEAST(observed_ms,${now})`;
}
// Keep listeners separate from the sender; telemetry failures must never trigger resends.
export function bindV16Receipts(sock: WASocket, sessionId: string) {
    let cached: string | undefined;
    async function dbId() {
        if (!cached) cached = (await prisma.session.findUnique({ where: { sessionId }, select: { id: true } }))?.id;
        return cached;
    }
    async function save(key: any, kind: ReceiptKind | undefined, user?: string | null) {
        if (!kind || key?.fromMe !== true || !validKey(key?.id)) return;
        try { const id = await dbId(); if (id) await record(id, key, kind, user); }
        catch { console.error('[TAZORIXX V16] Receipt persistence failed; check database access.'); }
    }
    sock.ev.on('messages.update', async updates => {
        for (const item of updates) await save(item.key, v16Status(item.update?.status));
    });
    sock.ev.on('messages.upsert', async ({messages}) => {
        for (const msg of messages) await save(msg.key, v16Status(msg.status));
    });
    sock.ev.on('message-receipt.update', async updates => {
        for (const {key,receipt} of updates) {
            if (!receipt.userJid) continue;
            if (Number(receipt.receiptTimestamp || 0) > 0) await save(key,'DELIVERED',receipt.userJid);
            if (Number(receipt.readTimestamp || 0) > 0 || Number(receipt.playedTimestamp || 0) > 0) await save(key,'READ',receipt.userJid);
        }
    });
}
export async function v16Lookup(dbSessionId: string, keyId: string, jid: string) {
    const receipts = await prisma.$queryRaw<{kind: string, observed_ms: bigint}[]>`
        SELECT kind,observed_ms FROM tazorixx_v16_receipts
        WHERE session_id=${dbSessionId} AND key_id=${keyId} AND jid=${jid}`;
    const events: Record<string,string> = {};
    for (const row of receipts) {
        if (['SENT','DELIVERED','READ','FAILED'].includes(row.kind)) events[row.kind] = new Date(Number(row.observed_ms)).toISOString();
    }
    const outgoing = await prisma.message.findUnique({where:{sessionId_keyId:{sessionId:dbSessionId,keyId}},
        select:{fromMe:true,remoteJid:true,timestamp:true}});
    // Never attach another chat's response or status. Historical Message.status is not trusted.
    if (outgoing && (!outgoing.fromMe || await canonicalV16Jid(outgoing.remoteJid,dbSessionId) !== jid)) {
        return {keyId,jid,events:{},reply:null,matched:false};
    }
    if (!outgoing) return {keyId,jid,events,reply:null,matched:!!receipts.length};
    // Exact quoted replies first; only within the same private conversation.
    // Use the existing message store, not a new copy of the entire inbox.
    const aliases = [...new Set([jid,outgoing.remoteJid])];
    const conversation = {sessionId:dbSessionId,remoteJid:{in:aliases}};
    const after = {gt:outgoing.timestamp};
    const select = {keyId:true,content:true,type:true,timestamp:true};
    let reply = await prisma.message.findFirst({where:{...conversation,fromMe:false,quoteId:keyId,timestamp:{gte:outgoing.timestamp}},orderBy:[{timestamp:'asc'},{keyId:'asc'}],select});
    let kind = 'QUOTED';
    if (!reply) {
        // Unquoted messages are conversation responses, not proven replies to a particular message.
        // Only associate with the last outbound message before the response, within seven days.
        // Ties at identical timestamps are ambiguous and intentionally excluded.
        const next = await prisma.message.findFirst({where:{...conversation,fromMe:true,keyId:{not:keyId},timestamp:{gte:outgoing.timestamp}},orderBy:{timestamp:'asc'},select:{timestamp:true}});
        const limit = new Date(outgoing.timestamp.getTime()+7*86400000);
        const upper = next && next.timestamp < limit ? next.timestamp : limit;
        if (upper > outgoing.timestamp) reply = await prisma.message.findFirst({where:{...conversation,fromMe:false,quoteId:null,timestamp:{...after,lt:upper}},orderBy:[{timestamp:'asc'},{keyId:'asc'}],select});
        kind = 'CONVERSATION';
    }
    return {keyId,jid,events,matched:true,reply:reply?{keyId:reply.keyId,kind,at:reply.timestamp.toISOString(),text:(reply.content || `[${reply.type}]`).slice(0,2000)}:null};
}
