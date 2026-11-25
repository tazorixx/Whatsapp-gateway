import { NextRequest, NextResponse } from 'next/server';
import { getAuthenticatedUser, canAccessSession } from '@/lib/api-auth';
import { prisma } from '@/lib/prisma';
import { ensureV16Table, v16Lookup } from '@/modules/whatsapp/tracking-v16';
import { directJid, validKey } from '@/modules/whatsapp/tracking-v16-logic';

// Read-only status query. No message sending, credentials, or public lead listing.
export async function POST(request: NextRequest, {params}: {params: Promise<{sessionId:string}>}) {
    try {
        const user = await getAuthenticatedUser(request);
        if (!user) return NextResponse.json({status:false,message:'Unauthorized'},{status:401});
        const {sessionId} = await params;
        if (!await canAccessSession(user.id,user.role,sessionId)) return NextResponse.json({status:false,message:'Forbidden'},{status:403});
        const session = await prisma.session.findUnique({where:{sessionId},select:{id:true}});
        if (!session) return NextResponse.json({status:false,message:'Session not found'},{status:404});
        const raw = await request.text();
        if (raw.length > 16000) return NextResponse.json({status:false,message:'Request too large'},{status:413});
        let body:any; try {body=JSON.parse(raw);} catch {return NextResponse.json({status:false,message:'Invalid JSON'},{status:400});}
        if (!Array.isArray(body?.items) || body.items.length<1 || body.items.length>25 || body.items.some((x:any)=>!validKey(x?.keyId)||directJid(x?.jid)!==x?.jid))
            return NextResponse.json({status:false,message:'Supply 1-25 exact message IDs and direct recipient JIDs'},{status:400});
        await ensureV16Table();
        const data=[];
        for (const x of body.items) data.push(await v16Lookup(session.id,x.keyId,x.jid));
        return NextResponse.json({status:true,version:16,data},{headers:{'Cache-Control':'no-store'}});
    } catch {
        return NextResponse.json({status:false,message:'V16 receipt query unavailable; check Gateway logs/database'},{status:503});
    }
}
