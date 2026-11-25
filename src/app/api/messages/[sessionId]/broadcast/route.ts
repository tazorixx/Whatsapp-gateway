import { NextResponse, NextRequest } from "next/server";
import { prisma } from "@/lib/prisma";
import { waManager } from "@/modules/whatsapp/manager";
import { getAuthenticatedUser, canAccessSession } from "@/lib/api-auth";
import { normalizeRecipients, MAX_BROADCAST_RECIPIENTS } from "@/modules/whatsapp/broadcast-utils";
import { z } from "zod";

const broadcastBodySchema = z.object({
    recipients: z.array(z.string().min(1)).min(1).max(MAX_BROADCAST_RECIPIENTS),
    message: z.string().trim().min(1).max(65536),
    delay: z.number().int().min(1000).max(15000).default(2000)
});

export async function POST(request: NextRequest, { params }: { params: Promise<{ sessionId: string }> }) {
    try {
        const user = await getAuthenticatedUser(request);
        if (!user) return NextResponse.json({ status: false, message: "Unauthorized" }, { status: 401 });
        const { sessionId } = await params;
        if (!await canAccessSession(user.id, user.role, sessionId)) {
            return NextResponse.json({ status: false, message: "Forbidden" }, { status: 403 });
        }
        const parsed = broadcastBodySchema.safeParse(await request.json());
        if (!parsed.success) {
            return NextResponse.json({ status: false, message: "Invalid recipients, message or delay", errors: parsed.error.flatten() }, { status: 400 });
        }
        let recipients: string[];
        try { recipients = normalizeRecipients(parsed.data.recipients); }
        catch (e: any) { return NextResponse.json({ status: false, message: e.message }, { status: 400 }); }

        const session = await prisma.session.findUnique({ where: { sessionId }, select: { status: true } });
        if (!session) return NextResponse.json({ status: false, message: "Session not found" }, { status: 404 });
        if (session.status === "LOGGED_OUT" || session.status === "STOPPED") {
            return NextResponse.json({ status: false, message: "Start and connect this WhatsApp session before broadcasting" }, { status: 409 });
        }

        // Avoid accidental duplicate submissions while the same batch is still active.
        const existing = await prisma.broadcastLog.findFirst({
            where: { sessionId, message: parsed.data.message,
                status: { in: ["queued", "running", "paused"] },
                startedAt: { gte: new Date(Date.now() - 60_000) } },
            select: { id: true }
        });
        if (existing) return NextResponse.json({ status: false, message: "A matching batch is already active", data: { broadcastId: existing.id } }, { status: 409 });

        const log = await prisma.broadcastLog.create({
            data: {
                sessionId,
                message: parsed.data.message,
                total: recipients.length,
                delay: parsed.data.delay,
                status: "queued",
                recipients: { create: recipients.map(jid => ({ jid, status: "pending" })) }
            }
        });
        const instance = waManager.getInstance(sessionId);
        const ready = instance?.status === "CONNECTED" && Boolean(instance.socket?.user);
        return NextResponse.json({
            status: true,
            message: ready ? "Broadcast queued and starting" : "Broadcast queued; will resume automatically after WhatsApp reconnects",
            data: { broadcastId: log.id, total: recipients.length, queued: !ready }
        }, { status: 202 });
    } catch (error) {
        console.error("Broadcast creation failed", error);
        return NextResponse.json({ status: false, message: "Failed to queue broadcast" }, { status: 500 });
    }
}
