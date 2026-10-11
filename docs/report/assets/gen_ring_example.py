import math
from pathlib import Path
COL={'P':'#a07a12','A':'#c0532b','B':'#1f6f8b','C':'#3d7d4f','D':'#6a4c93'}
INK='#1b2430'; MUTED='#5d6874'; RULE='#e3dfd7'; PAPER='#fff'
FONT='font-family="PingFang SC, Noto Sans SC, Microsoft YaHei, system-ui, sans-serif"'
PW=470; GAP=70; W=PW*2+GAP; H=330
R=185; BW=24; CY=300
def ang(p): return -80+p*160/90
def pt(cx,r,a):
    t=math.radians(a); return cx+r*math.sin(t), CY-r*math.cos(t)
def band(cx,a0,a1,r1,r2):
    lg=1 if a1-a0>180 else 0
    x1,y1=pt(cx,r2,a0);x2,y2=pt(cx,r2,a1);x3,y3=pt(cx,r1,a1);x4,y4=pt(cx,r1,a0)
    return f"M{x1:.1f} {y1:.1f}A{r2} {r2} 0 {lg} 1 {x2:.1f} {y2:.1f}L{x3:.1f} {y3:.1f}A{r1} {r1} 0 {lg} 0 {x4:.1f} {y4:.1f}Z"
def arc(cx,a0,a1,r):
    x1,y1=pt(cx,r,a0);x2,y2=pt(cx,r,a1)
    return f"M{x1:.1f} {y1:.1f}A{r} {r} 0 0 1 {x2:.1f} {y2:.1f}"
def text(x,y,s,size=13,fill=INK,weight=500,anchor='middle',extra=''):
    return f'<text x="{x:.1f}" y="{y:.1f}" font-size="{size}" font-weight="{weight}" fill="{fill}" text-anchor="{anchor}" {FONT} {extra}>{s}</text>'

def panel(ox, title, ranges, tokens, gone, sel, walk, center, takeover=None):
    cx=ox+PW/2; out=[]
    out.append(text(ox+18,34,title,15,INK,700,'start'))
    # faint continuation of the ring on both sides
    for a0,a1 in ((-91,-81),(81,91)):
        out.append(f'<path d="{arc(cx,a0,a1,R+BW/2)}" stroke="#c9c3b8" stroke-width="3" fill="none" stroke-linecap="round" stroke-dasharray="0.1 9"/>')
    for s,e,owner,op in ranges:
        a0,a1=ang(s),ang(e)
        stroke=f'stroke="{INK}" stroke-width="2.2"' if (s,e)==sel else f'stroke="{PAPER}" stroke-width="1.5"'
        out.append(f'<path d="{band(cx,a0,a1,R,R+BW)}" fill="{COL.get(owner,RULE)}" fill-opacity="{op}" {stroke}/>')
    # walk overlay
    rw=R+BW+22
    first=walk[0][0]; last=walk[-1][0]
    out.append(f'<path d="{arc(cx,ang(first),ang(last)+7,rw)}" fill="none" stroke="{INK}" stroke-width="1.4" stroke-dasharray="3 4" opacity=".55" marker-end="url(#arr)"/>')
    k=0
    for p,node,counted in walk:
        x,y=pt(cx,rw,ang(p))
        if counted:
            k+=1
            out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="11" fill="{COL[node]}" stroke="{PAPER}" stroke-width="2"/>')
            out.append(text(x,y+4.5,str(k),12,'#fff',700))
        else:
            out.append(text(x,y+5,'×',15,MUTED,700))
    if takeover:
        x,y=pt(cx,rw,ang(takeover))
        out.append(f'<rect x="{x-17:.1f}" y="{y-33:.1f}" width="34" height="17" rx="4" fill="{INK}"/>')
        out.append(text(x,y-20.5,'接管',11,PAPER,700))
    # tokens
    for p,node in tokens:
        x,y=pt(cx,R+BW/2,ang(p)); lx,ly=pt(cx,R-18,ang(p))
        if node in gone:
            out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{PAPER}" stroke="{MUTED}" stroke-width="1.5" opacity=".5"/>')
            out.append(text(lx,ly+4,f'{node}{p}',12,MUTED,600,extra='opacity=".45" text-decoration="line-through"'))
        else:
            out.append(f'<circle cx="{x:.1f}" cy="{y:.1f}" r="6" fill="{COL[node]}" stroke="{PAPER}" stroke-width="2"/>')
            out.append(text(lx,ly+4,f'{node}{p}',12,MUTED,600))
    # centre text
    y=CY-62
    for s,size,fill,weight in center:
        out.append(text(cx,y,s,size,fill,weight)); y+=size+12
    return out

TOK=[(10,'P'),(20,'A'),(25,'A'),(40,'B'),(60,'C'),(80,'D')]
before=[(0,10,'P',.45),(10,20,'A',1),(20,25,'A',1),(25,40,'B',1),(40,60,'C',1),(60,80,'D',1),(80,90,None,1)]
after=[(0,10,'P',.45),(10,40,'B',1),(40,60,'C',1),(60,80,'D',1),(80,90,None,1)]
svg=[f'<svg xmlns="http://www.w3.org/2000/svg" viewBox="0 0 {W} {H}" width="{W}" height="{H}" role="img" aria-label="移除节点 A 前后的区间与副本变化">',
     f'<defs><marker id="arr" viewBox="0 0 10 10" refX="8" refY="5" markerWidth="7" markerHeight="7" orient="auto-start-reverse"><path d="M0 0L10 5L0 10Z" fill="{INK}" opacity=".6"/></marker></defs>']
svg+=panel(0,'移除 A 之前',before,TOK,set(),(10,20),
           [(20,'A',True),(25,'A',False),(40,'B',True),(60,'C',True)],
           [('区间 (10, 20]',17,INK,700),('副本列表：A → B → C',13,INK,500),('A25 属于 A，跳过',12,MUTED,500)])
svg+=panel(PW+GAP,'移除 A 之后',after,TOK,{'A'},(10,40),
           [(40,'B',True),(60,'C',True),(80,'D',True)],
           [('合并区间 (10, 40]',17,INK,700),('副本列表：B → C → D',13,INK,500),('原第一个 follower B 成为 owner',12,MUTED,500)],
           takeover=40)
# middle arrow
mx=PW+GAP/2
svg.append(f'<path d="M{mx-24} {CY-150} L{mx+18} {CY-150}" stroke="{MUTED}" stroke-width="2" fill="none"/>')
svg.append(f'<path d="M{mx+18} {CY-156} L{mx+27} {CY-150} L{mx+18} {CY-144} Z" fill="{MUTED}"/>')
svg.append('</svg>')
(Path(__file__).resolve().parent / 'ring-example.svg').write_text('\n'.join(svg), encoding='utf-8')
