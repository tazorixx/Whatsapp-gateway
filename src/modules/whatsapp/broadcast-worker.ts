import { prisma } from "@/lib/prisma";
import { waManager } from "./manager";
import { logger } from "@/lib/logger";
import { MAX_SEND_ATTEMPTS, errorText, isTemporarySendError, retryDelayMs } from "./broadcast-utils";

/**
 * A single-process, database-backed broadcast runner. Jobs survive a process
 * restart; messages left in a state of unknown acceptance are NOT resent
 * automatically (that could duplicate messages). Run ONE app replica until a
 * distributed lease/lock is implemented.
 */
const busySessions = new Set<string>();
let scanner: ReturnType<typeof setInterval> | null = null;
let scanInProgress = false;

function connectedSocket(sessionId: string) {
    const instance = waManager.getInstance(sessionId);
    if (!instance || instance.isStopped || instance.status !== "CONNECTED" || !instance.socket?.user) return null;
    return instance.socket;
}

function sleep(ms: number) {
    return new Promise<void>(resolve => setTimeout(resolve, ms));
}

class AcceptanceUnknownError extends Error {}

async function sendWithDeadline(socket: NonNullable<ReturnType<typeof connectedSocket>>, jid: string, text: string) {
    let timeout: ReturnType<typeof setTimeout> | undefined;
    try {
        return await Promise.race([
            socket.sendMessage(jid, { text }),
            new Promise<never>((_, reject) => {
                timeout = setTimeout(() => reject(new AcceptanceUnknownError("Send timed out: WhatsApp may still accept this message")), 45_000);
            })
        ]);
    } finally {
        if (timeout) clearTimeout(timeout);
    }
}

async function emitProgress(id: string, sessionId: string, status?: string, current?: string) {
    const [log, recipients] = await Promise.all([
        prisma.broadcastLog.findUnique({ where: { id } }),
        prisma.broadcastRecipient.findMany({ where: { broadcastLogId: id }, select: { jid: true, status: true, error: true } })
    ]);
    if (!log) return;
    const sent = recipients.filter(r => r.status === "sent").length;
    const failed = recipients.filter(r => r.status === "failed" || r.status === "unknown").length;
    const effectiveStatus = status || log.status;
    await prisma.broadcastLog.update({
        where: { id },
        data: { sent, failed, status: effectiveStatus,
            ...(effectiveStatus === "completed" ? { completedAt: new Date() } : {}) }
    });
    const errors = recipients.filter(r => (r.status === "failed" || r.status === "unknown") && r.error)
        .slice(0, 100).map(r => ({ jid: r.jid, error: r.error || "Not confirmed" }));
    (global as any).io?.to(sessionId).emit("broadcast.progress", {
        broadcastId: id,
        // Keep the existing dashboard's event contract unchanged.
        status: effectiveStatus === "completed" ? "completed" : "running",
        paused: effectiveStatus === "paused",
        total: log.total, sent, failed, current: current || null,
        progress: Math.round(((sent + failed) / log.total) * 100),
        errors: effectiveStatus === "completed" ? errors : undefined,
        ...(effectiveStatus === "completed" ? { completedAt: new Date().toISOString() } : {})
    });
}

async function processBroadcast(id: string, sessionId: string) {
    const log = await prisma.broadcastLog.findUnique({ where: { id } });
    if (!log || log.status === "completed") return;

    if (!connectedSocket(sessionId)) {
        await emitProgress(id, sessionId, "paused");
        return; // The scanner automatically resumes when the connection opens.
    }
    await emitProgress(id, sessionId, "running");

    const recipients = await prisma.broadcastRecipient.findMany({
        where: { broadcastLogId: id }, orderBy: { id: "asc" }
    });

    for (let i = 0; i < recipients.length; i++) {
        const recipient = recipients[i];
        if (recipient.status === "sent" || recipient.status === "failed" || recipient.status === "unknown") continue;
        if (recipient.status === "sending") {
            // A previous process ended while acceptance was uncertain.
            await prisma.broadcastRecipient.update({
                where: { id: recipient.id },
                data: { status: "unknown", error: "App restarted mid-send. Delivery is uncertain; verify before retrying." }
            });
            await emitProgress(id, sessionId, "running", recipient.jid);
            continue;
        }

        let finished = false;
        for (let attempt = 1; attempt <= MAX_SEND_ATTEMPTS && !finished; attempt++) {
            const socket = connectedSocket(sessionId); // NEVER keep a stale socket across recipients.
            if (!socket) {
                await prisma.broadcastRecipient.update({ where: { id: recipient.id }, data: { status: "pending" } });
                await emitProgress(id, sessionId, "paused", recipient.jid);
                return;
            }
            await prisma.broadcastRecipient.update({
                where: { id: recipient.id }, data: { status: "sending", error: null }
            });
            try {
                const result = await sendWithDeadline(socket, recipient.jid, log.message);
                if (!result?.key?.id) {
                    // Do not blindly retry an ambiguous response.
                    await prisma.broadcastRecipient.update({
                        where: { id: recipient.id },
                        data: { status: "unknown", error: "Send returned no message ID; acceptance unconfirmed." }
                    });
                } else {
                    await prisma.broadcastRecipient.update({
                        where: { id: recipient.id },
                        data: { status: "sent", sentAt: new Date(), error: null }
                    });
                }
                finished = true;
            } catch (err) {
                const error = errorText(err);
                if (err instanceof AcceptanceUnknownError) {
                    await prisma.broadcastRecipient.update({ where: { id: recipient.id },
                        data: { status: "unknown", error } });
                    finished = true;
                    continue;
                }
                const temporary = isTemporarySendError(err);
                if (temporary && !connectedSocket(sessionId)) {
                    await prisma.broadcastRecipient.update({
                        where: { id: recipient.id }, data: { status: "pending", error }
                    });
                    logger.warn("Broadcast", `Pausing ${id} — session ${sessionId} disconnected: ${error}`);
                    await emitProgress(id, sessionId, "paused", recipient.jid);
                    return;
                }
                if (temporary && attempt < MAX_SEND_ATTEMPTS) {
                    await prisma.broadcastRecipient.update({
                        where: { id: recipient.id }, data: { status: "pending", error: `Retry ${attempt}: ${error}` }
                    });
                    await sleep(retryDelayMs(attempt));
                } else {
                    await prisma.broadcastRecipient.update({
                        where: { id: recipient.id }, data: { status: "failed", error }
                    });
                    finished = true;
                }
            }
        }
        await emitProgress(id, sessionId, "running", recipient.jid);
        // Send promptly, but keep a paced single-session queue to reduce overload.
        if (i < recipients.length - 1) await sleep(Math.max(1000, Math.min(15000, log.delay)));
    }
    await emitProgress(id, sessionId, "completed");
    logger.info("Broadcast", `Completed broadcast ${id} for ${sessionId}`);
}

async function scan() {
    if (scanInProgress) return;
    scanInProgress = true;
    try {
        const jobs = await prisma.broadcastLog.findMany({
            where: { status: { in: ["queued", "running", "paused"] } },
            orderBy: { startedAt: "asc" },
            take: 100
        });
        for (const job of jobs) {
            if (busySessions.has(job.sessionId)) continue;
            busySessions.add(job.sessionId);
            void processBroadcast(job.id, job.sessionId)
                .catch(err => logger.error("Broadcast", `Worker job ${job.id} failed:`, err))
                .finally(() => busySessions.delete(job.sessionId));
        }
    } catch (err) {
        logger.error("Broadcast", "Unable to scan queued jobs", err);
    } finally {
        scanInProgress = false;
    }
}

export function startBroadcastWorker() {
    if (scanner) return;
    scanner = setInterval(() => { void scan(); }, 1000);
    void scan();
    logger.info("Broadcast", "Recovery worker started (single-server mode)");
}

