import os, re, json, sqlite3, hashlib, secrets, base64, socket, struct, threading, time
from datetime import datetime, timezone
from http.server import ThreadingHTTPServer, BaseHTTPRequestHandler
from urllib.parse import urlparse, parse_qs
DB='/app/data/huddle.db'; os.makedirs('/app/data',exist_ok=True)
lock=threading.RLock(); clients=[]

def now(): return datetime.now(timezone.utc).isoformat(timespec='milliseconds').replace('+00:00','Z')
def db():
    c=sqlite3.connect(DB,check_same_thread=False); c.row_factory=sqlite3.Row; return c
def init():
    with lock:
        c=db()
        c.executescript('''CREATE TABLE IF NOT EXISTS users(id INTEGER PRIMARY KEY,username TEXT UNIQUE,password TEXT,display_name TEXT,timezone TEXT DEFAULT 'UTC',avatar_url TEXT DEFAULT '',status_text TEXT DEFAULT '',status_emoji TEXT DEFAULT '');
        CREATE TABLE IF NOT EXISTS tokens(token TEXT PRIMARY KEY,user_id INTEGER);
        CREATE TABLE IF NOT EXISTS workspaces(id INTEGER PRIMARY KEY,slug TEXT UNIQUE,name TEXT,owner_id INTEGER,join_mode TEXT DEFAULT 'open');
        CREATE TABLE IF NOT EXISTS members(workspace_id INTEGER,user_id INTEGER,role TEXT,PRIMARY KEY(workspace_id,user_id));
        CREATE TABLE IF NOT EXISTS channels(id INTEGER PRIMARY KEY,workspace_id INTEGER,name TEXT,is_private INTEGER DEFAULT 0,is_dm INTEGER DEFAULT 0,topic TEXT DEFAULT '',is_archived INTEGER DEFAULT 0);
        CREATE TABLE IF NOT EXISTS channel_members(channel_id INTEGER,user_id INTEGER,PRIMARY KEY(channel_id,user_id));
        CREATE TABLE IF NOT EXISTS messages(id INTEGER PRIMARY KEY,channel_id INTEGER,author_id INTEGER,body TEXT,parent_id INTEGER,created_at TEXT,edited_at TEXT,deleted INTEGER DEFAULT 0,event_id INTEGER);
        CREATE TABLE IF NOT EXISTS reactions(message_id INTEGER,user_id INTEGER,emoji TEXT,UNIQUE(message_id,user_id,emoji));
        CREATE TABLE IF NOT EXISTS pins(message_id INTEGER PRIMARY KEY,user_id INTEGER,pinned_at TEXT);
        CREATE TABLE IF NOT EXISTS files(id INTEGER PRIMARY KEY,uploader_id INTEGER,filename TEXT,content_type TEXT,size INTEGER,created_at TEXT,data BLOB);
        CREATE TABLE IF NOT EXISTS seq(channel_id INTEGER PRIMARY KEY,event_id INTEGER DEFAULT 0); CREATE TABLE IF NOT EXISTS invitations(code TEXT PRIMARY KEY,workspace_id INTEGER,creator_id INTEGER,invited_user_id INTEGER,expires_at TEXT,max_uses INTEGER DEFAULT 1,uses INTEGER DEFAULT 0,revoked INTEGER DEFAULT 0);'''); c.commit(); c.close()
def userobj(r):
    return {k:r[k] for k in ('id','username','display_name','timezone','avatar_url','status_text','status_emoji')}
def msgobj(c,r):
    a=c.execute('select * from users where id=?',(r['author_id'],)).fetchone()
    rx=[]
    for x in c.execute('select emoji,count(*) n from reactions where message_id=? group by emoji',(r['id'],)):
        ids=[z[0] for z in c.execute('select user_id from reactions where message_id=? and emoji=?',(r['id'],x['emoji']))]
        rx.append({'emoji':x['emoji'],'count':x['n'],'user_ids':ids})
    replies=c.execute('select count(*) from messages where parent_id=? and deleted=0',(r['id'],)).fetchone()[0]
    return {'id':r['id'],'channel_id':r['channel_id'],'author_id':r['author_id'],'author':userobj(a),'body':r['body'],'parent_id':r['parent_id'],'reply_count':replies,'created_at':r['created_at'],'edited_at':r['edited_at'],'files':[],'reactions':rx,'mentions':[],'event_id':r['event_id']}
def broadcast(event):
    dead=[]
    with lock:
        for s,ch in clients:
            try:
                if ch is None or event.get('channel_id') in ch: ws_send(s,event)
            except: dead.append((s,ch))
        for x in dead:
            try: clients.remove(x); x[0].close()
            except: pass
def ws_send(s,o):
    b=json.dumps(o,separators=(',',':')).encode(); n=len(b)
    if n<126: h=bytes([0x81,n])
    elif n<65536: h=bytes([0x81,126])+struct.pack('>H',n)
    else: h=bytes([0x81,127])+struct.pack('>Q',n)
    s.sendall(h+b)
class H(BaseHTTPRequestHandler):
    server_version='Huddle/1'
    def log_message(self,*a): pass
    def sendj(self,status,obj,headers=None):
        b=json.dumps(obj).encode(); self.send_response(status); self.send_header('Content-Type','application/json'); self.send_header('Content-Length',str(len(b)))
        for k,v in (headers or {}).items(): self.send_header(k,v)
        self.end_headers(); self.wfile.write(b)
    def body(self):
        n=int(self.headers.get('Content-Length','0')); return json.loads(self.rfile.read(n) or b'{}')
    def auth(self):
        t=self.headers.get('Authorization','').replace('Bearer ','',1)
        c=db(); r=c.execute('select u.* from tokens t join users u on u.id=t.user_id where t.token=?',(t,)).fetchone(); c.close(); return r
    def path(self): return urlparse(self.path).path
    def do_GET(self):
        parsed=urlparse(self.path); p=parsed.path; q=parse_qs(parsed.query)
        if p=='/api/health': return self.sendj(200,{'status':'ok','node_id':self.server.node})
        if p=='/': return self.html()
        if p.startswith('/api/ws'): return self.websocket(q)
        u=self.auth()
        if p=='/api/auth/me':
            return self.sendj(200,{'user':userobj(u)}) if u else self.sendj(401,{'error':'unauthorized'})
        if not u: return self.sendj(401,{'error':'unauthorized'})
        c=db()
        if p=='/api/files':
            c=db(); raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            if len(raw)>10*1024*1024:return self.sendj(413,{'error':'file too large'})
            boundary=self.headers.get('Content-Type','').split('boundary=')[-1].encode()
            if not boundary:return self.sendj(400,{'error':'multipart required'})
            parts=raw.split(b'--'+boundary)
            data=None; filename='upload'; ctype='application/octet-stream'
            for part in parts:
                if b'filename=' not in part: continue
                head,sep,body=part.partition(b'\r\n\r\n')
                mm=re.search(br'filename="([^"]*)"',head); mt=re.search(br'Content-Type:\s*([^\r\n]+)',head,re.I)
                if mm:filename=mm.group(1).decode(errors='replace')
                if mt:ctype=mt.group(1).decode(errors='replace')
                data=body.rstrip(b'\r\n-')
            if data is None:return self.sendj(400,{'error':'file required'})
            t=now();c.execute('insert into files(uploader_id,filename,content_type,size,created_at,data) values(?,?,?,?,?,?)',(u['id'],filename,ctype,len(data),t,data));fid=c.execute('select last_insert_rowid()').fetchone()[0];c.commit()
            return self.sendj(201,{'file':{'id':fid,'uploader_id':u['id'],'filename':filename,'content_type':ctype,'size':len(data),'created_at':t}})
        if p=='/api/workspaces':
            rows=c.execute('select * from workspaces w join members m on m.workspace_id=w.id where m.user_id=?',(u['id'],)).fetchall()
            return self.sendj(200,{'workspaces':[dict(r) for r in rows]})
        m=re.match(r'^/api/workspaces/([^/]+)/members/(\d+)$',p)
        if m:
            w=c.execute('select id from workspaces where slug=?',(m.group(1),)).fetchone()
            actor=c.execute('select role from members where workspace_id=? and user_id=?',(w['id'],u['id'])).fetchone() if w else None
            if not w:return self.sendj(404,{'error':'not found'})
            if not actor or actor['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            role=b.get('role')
            if role not in ('admin','member','guest'):return self.sendj(400,{'error':'invalid role'})
            target=c.execute('select user_id from members where workspace_id=? and user_id=?',(w['id'],m.group(2))).fetchone()
            if not target:return self.sendj(404,{'error':'not found'})
            c.execute('update members set role=? where workspace_id=? and user_id=?',(role,w['id'],m.group(2)));c.commit()
            return self.sendj(200,{'member':{'user_id':int(m.group(2)),'role':role}})
        m=re.match(r'^/api/workspaces/([^/]+)$',p)
        if m:
            w=c.execute('select * from workspaces where slug=?',(m.group(1),)).fetchone()
            if not w:return self.sendj(404,{'error':'not found'})
            cs=c.execute('select * from channels where workspace_id=? and (is_archived=0)',(w['id'],)).fetchall()
            states=[{'channel_id':x['id'],'last_read_event_id':0,'unread_count':0,'mention_count':0} for x in cs]
            return self.sendj(200,{'workspace':dict(w),'channels':[dict(x) for x in cs],'read_state':states})
        m=re.match(r'^/api/channels/(\d+)/messages$',p)
        if m:
            rs=c.execute('select * from messages where channel_id=? and deleted=0 and parent_id is null order by id desc limit 200',(m.group(1),)).fetchall()
            return self.sendj(200,{'messages':[msgobj(c,x) for x in rs],'next_cursor':None})
        m=re.match(r'^/api/messages/(\d+)/replies$',p)
        if m:
            rs=c.execute('select * from messages where parent_id=? and deleted=0 order by id',(m.group(1),))
            return self.sendj(200,{'replies':[msgobj(c,x) for x in rs],'next_cursor':None})
        m=re.match(r'^/api/channels/(\d+)/members$',p)
        if m:
            rs=c.execute('select u.* from users u join channel_members m on m.user_id=u.id where m.channel_id=?',(m.group(1),))
            return self.sendj(200,{'members':[userobj(x) for x in rs]})
        m=re.match(r'^/api/users/(\d+)$',p)
        if m:
            r=c.execute('select * from users where id=?',(m.group(1),)).fetchone(); return self.sendj(200,{'user':userobj(r)}) if r else self.sendj(404,{'error':'not found'})
        m=re.match(r'^/api/workspaces/([^/]+)/members$',p)
        if m:
            w=c.execute('select id from workspaces where slug=?',(m.group(1),)).fetchone()
            if not w:return self.sendj(404,{'error':'not found'})
            rs=c.execute('select u.*,m.role from users u join members m on m.user_id=u.id where m.workspace_id=?',(w['id'],)).fetchall()
            return self.sendj(200,{'members':[dict(userobj(x),user_id=x['id'],role=x['role']) for x in rs]})
        if p=='/api/search':
            text=q.get('q',[''])[0]
            rs=c.execute('select m.* from messages m join channel_members cm on cm.channel_id=m.channel_id where cm.user_id=? and m.deleted=0 and m.body like ? order by m.id desc limit 200',(u['id'],'%'+text+'%')).fetchall()
            return self.sendj(200,{'results':[msgobj(c,x) for x in rs],'next_cursor':None})
        m=re.match(r'^/api/workspaces/([^/]+)/invitations$',p)
        if m:
            w=c.execute('select id from workspaces where slug=?',(m.group(1),)).fetchone()
            role=c.execute('select role from members where workspace_id=? and user_id=?',(w['id'],u['id'])) if w else None
            if not w:return self.sendj(404,{'error':'not found'})
            if not role or role.fetchone()['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            rs=c.execute('select * from invitations where workspace_id=? and revoked=0 and uses<max_uses',(w['id'],)).fetchall()
            return self.sendj(200,{'invitations':[dict(x) for x in rs]})
        m=re.match(r'^/api/channels/(\d+)/pins$',p)
        if m:
            rs=c.execute('select p.*,m.* from pins p join messages m on m.id=p.message_id where m.channel_id=? order by p.pinned_at desc',(m.group(1),)).fetchall()
            return self.sendj(200,{'pins':[{'message':msgobj(c,x),'pinned_by':x['user_id'],'pinned_at':x['pinned_at']} for x in rs],'next_cursor':None})
        m=re.match(r'^/api/channels/(\d+)/members$',p)
        if m:
            rs=c.execute('select u.* from users u join channel_members cm on cm.user_id=u.id where cm.channel_id=?',(m.group(1),)).fetchall()
            return self.sendj(200,{'members':[userobj(x) for x in rs]})
        m=re.match(r'^/api/workspaces/([^/]+)/invitations$',p)
        if m:
            w=c.execute('select * from workspaces where slug=?',(m.group(1),)).fetchone()
            role=c.execute('select role from members where workspace_id=? and user_id=?',(w['id'],u['id'])).fetchone() if w else None
            if not w:return self.sendj(404,{'error':'not found'})
            if not role or role['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            code=secrets.token_urlsafe(12); target=b.get('invited_user_id')
            exp=datetime.fromtimestamp(time.time()+int(b.get('expires_in_seconds',604800)),timezone.utc).isoformat()
            c.execute('insert into invitations values(?,?,?,?,?,?,?,0)',(code,w['id'],u['id'],target,exp,int(b.get('max_uses',1)),0));c.commit()
            return self.sendj(201,{'invitation':dict(c.execute('select * from invitations where code=?',(code,)).fetchone())})
        m=re.match(r'^/api/invitations/([^/]+)/accept$',p)
        if m:
            inv=c.execute('select * from invitations where code=?',(m.group(1),)).fetchone()
            if not inv or inv['revoked'] or inv['uses']>=inv['max_uses'] or inv['expires_at']<now():return self.sendj(404,{'error':'not found'})
            if inv['invited_user_id'] and inv['invited_user_id']!=u['id']:return self.sendj(403,{'error':'forbidden'})
            c.execute('insert or ignore into members values(?,?,?)',(inv['workspace_id'],u['id'],'member'));c.execute('update invitations set uses=uses+1 where code=?',(inv['code'],));c.commit()
            return self.sendj(200,{'workspace':dict(c.execute('select * from workspaces where id=?',(inv['workspace_id'],)).fetchone())})
        m=re.match(r'^/api/channels/(\d+)/read$',p)
        if m:
            return self.sendj(200,{'read_state':{'channel_id':int(m.group(1)),'last_read_event_id':0,'unread_count':0,'mention_count':0}})
        m=re.match(r'^/api/files/(\d+)/download$',p)
        if m:
            f=c.execute('select * from files where id=?',(m.group(1),)).fetchone()
            if not f:return self.sendj(404,{'error':'not found'})
            self.send_response(200);self.send_header('Content-Type',f['content_type']);self.send_header('Content-Disposition','attachment; filename="'+f['filename']+'"');self.send_header('Content-Length',str(f['size']));self.end_headers();self.wfile.write(f['data']);return
        m=re.match(r'^/api/files/(\d+)$',p)
        if m:
            f=c.execute('select id,uploader_id,filename,content_type,size,created_at from files where id=?',(m.group(1),)).fetchone()
            return self.sendj(200,{'file':dict(f)}) if f else self.sendj(404,{'error':'not found'})
        return self.sendj(404,{'error':'not found'})
    def do_POST(self):
        p=self.path; b={} if self.headers.get('Content-Type','').startswith('multipart/form-data') else self.body()
        if p=='/api/auth/register':
            if not re.fullmatch(r'[A-Za-z0-9_]+',b.get('username','')) or len(b.get('password',''))<8:return self.sendj(400,{'error':'invalid registration'})
            c=db()
            try:
                c.execute('insert into users(username,password,display_name) values(?,?,?)',(b['username'],hashlib.sha256(b['password'].encode()).hexdigest(),b.get('display_name') or b['username'])); uid=c.execute('select last_insert_rowid()').fetchone()[0]; t=secrets.token_urlsafe(24); c.execute('insert into tokens values(?,?)',(t,uid)); c.commit(); r=c.execute('select * from users where id=?',(uid,)).fetchone(); return self.sendj(201,{'user':userobj(r),'token':t})
            except sqlite3.IntegrityError:return self.sendj(409,{'error':'duplicate username'})
        if p=='/api/auth/login':
            c=db(); r=c.execute('select * from users where username=? and password=?',(b.get('username'),hashlib.sha256(b.get('password','').encode()).hexdigest())).fetchone()
            if not r:return self.sendj(401,{'error':'invalid credentials'})
            t=secrets.token_urlsafe(24); c.execute('insert into tokens values(?,?)',(t,r['id'])); c.commit(); return self.sendj(200,{'user':userobj(r),'token':t})
        u=self.auth()
        if not u:return self.sendj(401,{'error':'unauthorized'})
        c=db()
        if p=='/api/files':
            raw=self.rfile.read(int(self.headers.get('Content-Length','0')))
            if len(raw)>10*1024*1024:return self.sendj(413,{'error':'file too large'})
            ct=self.headers.get('Content-Type','')
            if 'boundary=' not in ct:return self.sendj(400,{'error':'multipart required'})
            boundary=ct.split('boundary=',1)[1].strip().strip('"').encode()
            data=None; filename='upload'; content_type='application/octet-stream'
            for part in raw.split(b'--'+boundary):
                if b'filename=' not in part: continue
                head,sep,body=part.partition(b'\r\n\r\n')
                fm=re.search(br'filename="([^"]*)"',head)
                tm=re.search(br'Content-Type:\s*([^\r\n]+)',head,re.I)
                if fm: filename=fm.group(1).decode('utf-8','replace')
                if tm: content_type=tm.group(1).decode('utf-8','replace')
                data=body.rstrip(b'\r\n-')
            if data is None:return self.sendj(400,{'error':'file required'})
            created=now()
            c.execute('insert into files(uploader_id,filename,content_type,size,created_at,data) values(?,?,?,?,?,?)',(u['id'],filename,content_type,len(data),created,data))
            fid=c.execute('select last_insert_rowid()').fetchone()[0];c.commit()
            return self.sendj(201,{'file':{'id':fid,'uploader_id':u['id'],'filename':filename,'content_type':content_type,'size':len(data),'created_at':created}})
        if p=='/api/workspaces':
            if not re.fullmatch(r'[a-z0-9-]{2,32}',b.get('slug','')):return self.sendj(400,{'error':'invalid slug'})
            try:
                c.execute('insert into workspaces(slug,name,owner_id) values(?,?,?)',(b['slug'],b.get('name',b['slug']),u['id'])); wid=c.execute('select last_insert_rowid()').fetchone()[0]; c.execute('insert into members values(?,?,?)',(wid,u['id'],'owner')); c.execute('insert into channels(workspace_id,name) values(?,?)',(wid,'general')); cid=c.execute('select last_insert_rowid()').fetchone()[0]; c.execute('insert into channel_members values(?,?)',(cid,u['id'])); c.commit(); w=c.execute('select * from workspaces where id=?',(wid,)).fetchone(); ch=c.execute('select * from channels where id=?',(cid,)).fetchone(); return self.sendj(201,{'workspace':dict(w),'general_channel':dict(ch)})
            except sqlite3.IntegrityError:return self.sendj(409,{'error':'duplicate slug'})
        m=re.match(r'^/api/workspaces/([^/]+)/channels$',p)
        if m:
            w=c.execute('select * from workspaces where slug=?',(m.group(1),)).fetchone()
            if not w:return self.sendj(404,{'error':'not found'})
            mem=c.execute('select role from members where workspace_id=? and user_id=?',(w['id'],u['id'])).fetchone()
            if not mem:return self.sendj(403,{'error':'forbidden'})
            if not re.fullmatch(r'[a-z0-9-]{1,32}',b.get('name','')):return self.sendj(400,{'error':'invalid name'})
            if b.get('is_private') and mem['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            try:
                c.execute('insert into channels(workspace_id,name,is_private,topic) values(?,?,?,?)',(w['id'],b['name'],int(bool(b.get('is_private'))),b.get('topic',''))); cid=c.execute('select last_insert_rowid()').fetchone()[0]; c.execute('insert into channel_members values(?,?)',(cid,u['id'])); c.commit(); return self.sendj(201,{'channel':dict(c.execute('select * from channels where id=?',(cid,)).fetchone())})
            except sqlite3.IntegrityError:return self.sendj(409,{'error':'duplicate channel'})
        m=re.match(r'^/api/channels/(\d+)/(archive|unarchive)$',p)
        if m:
            ch=c.execute('select * from channels where id=?',(m.group(1),)).fetchone()
            if not ch:return self.sendj(404,{'error':'not found'})
            role=c.execute('select role from members where workspace_id=? and user_id=?',(ch['workspace_id'],u['id'])).fetchone()
            if not role or role['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            val=1 if m.group(2)=='archive' else 0
            c.execute('update channels set is_archived=? where id=?',(val,ch['id']));c.commit()
            return self.sendj(200,{'channel':dict(c.execute('select * from channels where id=?',(ch['id'],)).fetchone())})
        m=re.match(r'^/api/workspaces/([^/]+)/invitations$',p)
        if m:
            w=c.execute('select * from workspaces where slug=?',(m.group(1),)).fetchone()
            role=c.execute('select role from members where workspace_id=? and user_id=?',(w['id'],u['id'])).fetchone() if w else None
            if not w:return self.sendj(404,{'error':'not found'})
            if not role or role['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            code=secrets.token_urlsafe(12); target=b.get('invited_user_id')
            exp=datetime.fromtimestamp(time.time()+int(b.get('expires_in_seconds',604800)),timezone.utc).isoformat()
            c.execute('insert into invitations values(?,?,?,?,?,?,?,0)',(code,w['id'],u['id'],target,exp,int(b.get('max_uses',1)),0));c.commit()
            return self.sendj(201,{'invitation':dict(c.execute('select * from invitations where code=?',(code,)).fetchone())})
        m=re.match(r'^/api/invitations/([^/]+)/accept$',p)
        if m:
            inv=c.execute('select * from invitations where code=?',(m.group(1),)).fetchone()
            if not inv or inv['revoked'] or inv['uses']>=inv['max_uses'] or inv['expires_at']<now():return self.sendj(404,{'error':'not found'})
            if inv['invited_user_id'] and inv['invited_user_id']!=u['id']:return self.sendj(403,{'error':'forbidden'})
            c.execute('insert or ignore into members values(?,?,?)',(inv['workspace_id'],u['id'],'member'));c.execute('update invitations set uses=uses+1 where code=?',(inv['code'],));c.commit()
            return self.sendj(200,{'workspace':dict(c.execute('select * from workspaces where id=?',(inv['workspace_id'],)).fetchone())})
        m=re.match(r'^/api/channels/(\d+)/read$',p)
        if m:
            ch=c.execute('select id from channels where id=?',(m.group(1),)).fetchone()
            if not ch:return self.sendj(404,{'error':'not found'})
            val=max(0,int(b.get('last_read_event_id',0)))
            return self.sendj(200,{'read_state':{'channel_id':int(m.group(1)),'last_read_event_id':val,'unread_count':0,'mention_count':0}})
        m=re.match(r'^/api/channels/(\d+)/join$',p)
        if m:
            ch=c.execute('select * from channels where id=?',(m.group(1),)).fetchone()
            if not ch:return self.sendj(404,{'error':'not found'})
            c.execute('insert or ignore into channel_members values(?,?)',(ch['id'],u['id'])); c.execute('insert or ignore into members values(?,?,?)',(ch['workspace_id'],u['id'],'member')); c.commit(); return self.sendj(200,{'channel':dict(ch)})
        m=re.match(r'^/api/messages/(\d+)/reactions$',p)
        if m:
            r=c.execute('select * from messages where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            emoji=str(b.get('emoji','')).strip()
            if not emoji:return self.sendj(400,{'error':'emoji required'})
            c.execute('insert or ignore into reactions(message_id,user_id,emoji) values(?,?,?)',(r['id'],u['id'],emoji));c.commit()
            return self.sendj(200,{'reactions':msgobj(c,c.execute('select * from messages where id=?',(r['id'],)).fetchone())['reactions']})
        m=re.match(r'^/api/messages/(\d+)/pin$',p)
        if m:
            r=c.execute('select * from messages where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            t=now();c.execute('insert or ignore into pins(message_id,user_id,pinned_at) values(?,?,?)',(r['id'],u['id'],t));c.commit()
            return self.sendj(200,{'pin':{'message_id':r['id'],'pinned_by':u['id'],'pinned_at':t}})
        m=re.match(r'^/api/channels/(\d+)/messages$',p)
        if m:
            ch=c.execute('select * from channels where id=?',(m.group(1),)).fetchone()
            if not ch:return self.sendj(404,{'error':'not found'})
            if not b.get('body','').strip():return self.sendj(400,{'error':'empty message'})
            with lock:
                c.execute('insert into seq(channel_id,event_id) values(?,1) on conflict(channel_id) do update set event_id=event_id+1',(ch['id'],)); eid=c.execute('select event_id from seq where channel_id=?',(ch['id'],)).fetchone()[0]
                c.execute('insert into messages(channel_id,author_id,body,parent_id,created_at,event_id) values(?,?,?,?,?,?)',(ch['id'],u['id'],b['body'],b.get('parent_id'),now(),eid)); mid=c.execute('select last_insert_rowid()').fetchone()[0]; c.commit(); r=c.execute('select * from messages where id=?',(mid,)).fetchone(); out=msgobj(c,r)
            broadcast({'type':'message.reply' if r['parent_id'] else 'message.created','event_id':eid,'channel_id':ch['id'],'message':out}); return self.sendj(201,{'message':out})
        return self.sendj(404,{'error':'not found'})
    def do_PATCH(self):
        u=self.auth()
        if not u:return self.sendj(401,{'error':'unauthorized'})
        p=self.path; b=self.body(); c=db()
        if p=='/api/users/me':
            allowed=('display_name','timezone','avatar_url','status_text','status_emoji')
            vals={k:b[k] for k in allowed if k in b}
            if vals:
                c.execute('update users set '+','.join(k+'=?' for k in vals)+' where id=?',tuple(vals.values())+(u['id'],)); c.commit()
            return self.sendj(200,{'user':userobj(c.execute('select * from users where id=?',(u['id'],)).fetchone())})
        m=re.match(r'^/api/workspaces/([^/]+)$',p)
        if m:
            w=c.execute('select * from workspaces where slug=?',(m.group(1),)).fetchone()
            if not w:return self.sendj(404,{'error':'not found'})
            role=c.execute('select role from members where workspace_id=? and user_id=?',(w['id'],u['id'])).fetchone()
            if not role or role['role'] not in ('owner','admin'):return self.sendj(403,{'error':'forbidden'})
            vals={k:b[k] for k in ('name','join_mode') if k in b}
            if 'join_mode' in vals and vals['join_mode'] not in ('open','invite_only'):return self.sendj(400,{'error':'invalid join mode'})
            if vals:
                c.execute('update workspaces set '+','.join(k+'=?' for k in vals)+' where id=?',tuple(vals.values())+(w['id'],));c.commit()
            return self.sendj(200,{'workspace':dict(c.execute('select * from workspaces where id=?',(w['id'],)).fetchone())})
        m=re.match(r'^/api/channels/(\d+)/read$',p)
        if m:
            val=int(b.get('last_read_event_id',0))
            return self.sendj(200,{'read_state':{'channel_id':int(m.group(1)),'last_read_event_id':max(0,val),'unread_count':0,'mention_count':0}})
        m=re.match(r'^/api/channels/(\d+)$',p)
        if m:
            ch=c.execute('select * from channels where id=?',(m.group(1),)).fetchone()
            if not ch:return self.sendj(404,{'error':'not found'})
            if 'topic' in b and len(str(b['topic']))>250:return self.sendj(400,{'error':'topic too long'})
            if 'topic' in b:c.execute('update channels set topic=? where id=?',(b['topic'],m.group(1)));c.commit()
            return self.sendj(200,{'channel':dict(c.execute('select * from channels where id=?',(m.group(1),)).fetchone())})
        m=re.match(r'^/api/messages/(\d+)$',p)
        if m:
            r=c.execute('select * from messages where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            if r['author_id']!=u['id']:return self.sendj(403,{'error':'forbidden'})
            c.execute('update messages set body=?,edited_at=? where id=?',(b.get('body',''),now(),r['id'])); c.commit(); return self.sendj(200,{'message':msgobj(c,c.execute('select * from messages where id=?',(r['id'],)).fetchone())})
        m=re.match(r'^/api/messages/(\d+)/reactions$',p)
        if m:
            r=c.execute('select * from messages where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            emoji=str(b.get('emoji','')).strip()
            if not emoji:return self.sendj(400,{'error':'emoji required'})
            c.execute('insert or ignore into reactions(message_id,user_id,emoji) values(?,?,?)',(r['id'],u['id'],emoji));c.commit()
            return self.sendj(200,{'reactions':msgobj(c,r)['reactions']})
        m=re.match(r'^/api/messages/(\d+)/pin$',p)
        if m:
            r=c.execute('select * from messages where id=?',(m.group(1),)).fetchone()
            if not r:return self.sendj(404,{'error':'not found'})
            t=now();c.execute('insert or ignore into pins(message_id,user_id,pinned_at) values(?,?,?)',(r['id'],u['id'],t));c.commit()
            return self.sendj(200,{'pin':{'message_id':r['id'],'pinned_by':u['id'],'pinned_at':t}})
        return self.sendj(404,{'error':'not found'})
    def do_DELETE(self):
        u=self.auth()
        if not u:return self.sendj(401,{'error':'unauthorized'})
        c=db(); m=re.match(r'^/api/messages/(\d+)$',self.path)
        if m:c.execute('update messages set deleted=1 where id=? and author_id=?',(m.group(1),u['id']));c.commit();return self.sendj(200,{'deleted':True})
        m=re.match(r'^/api/messages/(\d+)/reactions/(.+)$',self.path)
        if m:
            c.execute('delete from reactions where message_id=? and user_id=? and emoji=?',(m.group(1),u['id'],m.group(2)));c.commit()
            return self.sendj(200,{'reactions':[]})
        m=re.match(r'^/api/messages/(\d+)/pin$',self.path)
        if m:
            c.execute('delete from pins where message_id=?',(m.group(1),));c.commit()
            return self.sendj(200,{'unpinned':True})
        m=re.match(r'^/api/workspaces/([^/]+)/invitations/([^/]+)$',self.path)
        if m:
            c.execute('update invitations set revoked=1 where code=?',(m.group(2),));c.commit()
            return self.sendj(200,{'deleted':True})
        return self.sendj(404,{'error':'not found'})
    def websocket(self,q):
        u=self.auth() if not self.headers.get('Authorization') else self.auth()
        token=q.get('token',[''])[0].replace('Bearer ','')
        c=db(); good=c.execute('select 1 from tokens where token=?',(token,)).fetchone(); c.close()
        if not good:return self.send_response(401) or self.end_headers()
        key=self.headers.get('Sec-WebSocket-Key')
        if not key:return self.sendj(400,{'error':'websocket required'})
        self.send_response(101,'Switching Protocols'); self.send_header('Upgrade','websocket'); self.send_header('Connection','Upgrade'); self.send_header('Sec-WebSocket-Accept',base64.b64encode(hashlib.sha1((key+'258EAFA5-E914-47DA-95CA-C5AB0DC85B11').encode()).digest()).decode()); self.end_headers()
        s=self.connection; item=(s,set()); clients.append(item); ws_send(s,{'type':'ready'})
        s.settimeout(0.15); seen=set(); last_poll=0
        def frame():
            h=s.recv(2)
            if not h:return None
            ln=h[1]&127
            if ln==126: ln=struct.unpack('>H',s.recv(2))[0]
            elif ln==127: ln=struct.unpack('>Q',s.recv(8))[0]
            mask=s.recv(4); data=s.recv(ln); return bytes(data[i]^mask[i%4] for i in range(ln))
        try:
            while True:
                try:
                    raw=frame()
                    if raw:
                        z=json.loads(raw.decode())
                        typ=z.get('type'); cid=int(z.get('channel_id',0))
                        if typ in ('subscribe','resume') and cid:
                            item[1].add(cid)
                            c2=db(); head=c2.execute('select coalesce(event_id,0) from seq where channel_id=?',(cid,)).fetchone()[0]
                            if typ=='subscribe':
                                for oldrow in c2.execute('select event_id from messages where channel_id=? and event_id is not null',(cid,)):
                                    seen.add((cid,oldrow['event_id']))
                            if typ=='resume':
                                since=int(z.get('since_event_id',0))
                                rows=c2.execute('select * from messages where channel_id=? and event_id>? and deleted=0 order by event_id',(cid,since)).fetchall()
                                for rr in rows:
                                    ws_send(s,{'type':'message.reply' if rr['parent_id'] else 'message.created','event_id':rr['event_id'],'channel_id':cid,'message':msgobj(c2,rr)})
                                ws_send(s,{'type':'resumed','channel_id':cid,'head_event_id':head})
                            else: ws_send(s,{'type':'subscribed','channel_id':cid,'head_event_id':head})
                            c2.close()
                except socket.timeout: pass
                except (ConnectionError,ValueError,json.JSONDecodeError): break
                c2=db()
                for cid in list(item[1]):
                    rows=c2.execute('select * from messages where channel_id=? and event_id>? and deleted=0 order by event_id',(cid,last_poll)).fetchall()
                    for rr in rows:
                        key=(cid,rr['event_id'])
                        if key not in seen:
                            seen.add(key); ws_send(s,{'type':'message.reply' if rr['parent_id'] else 'message.created','event_id':rr['event_id'],'channel_id':cid,'message':msgobj(c2,rr)})
                last_poll=max(last_poll,c2.execute('select coalesce(max(event_id),0) from messages').fetchone()[0]); c2.close()
        except Exception: pass
        try: clients.remove(item)
        except: pass
    def html(self):
        x='''<!doctype html><html><head><meta charset="utf-8"><title>Huddle</title><style>
body{margin:0;font:14px Arial;color:#222}button{border:0;padding:9px 14px;border-radius:4px;color:#fff;cursor:pointer}.primary{background:#007a5a}.danger{background:#e01e5a}.secondary{background:#1264a3}.app{display:flex;height:100vh}.side{width:250px;background:#3f0f3f;color:white;padding:18px}.main{flex:1;padding:28px}.row{padding:10px;border-bottom:1px solid #ddd}.muted{color:#777}input,textarea{padding:10px;margin:5px;width:260px}#thread-panel{position:fixed;right:0;top:0;width:330px;height:100vh;background:#fff;border-left:1px solid #ddd;padding:20px;box-sizing:border-box;z-index:5}.picker{display:flex;gap:6px;background:#fff;border:1px solid #ccc;padding:8px;position:absolute}.picker button{background:#eee;color:#222;padding:7px}</style></head><body><div id="root"></div><script>
const A='/api';let token=localStorage.getItem('huddle.token'),me,ws,cur;
async function api(p,o={}){o.headers={...(o.headers||{}),...(token?{Authorization:'Bearer '+token}:{})};if(o.body&&typeof o.body!='string'){o.headers['Content-Type']='application/json';o.body=JSON.stringify(o.body)}let r=await fetch(A+p,o);return [r,await r.json().catch(()=>({}))]}
function auth(){let reg=false;root.innerHTML='<div id="auth-modal" style="padding:40px"><h1>Huddle</h1><h2 id="auth-title">Sign in</h2><form data-testid="auth-form"><input name="username" placeholder="username" required><input name="password" type="password" placeholder="password" required><input name="display_name" placeholder="display name"><button class="primary" data-testid="auth-submit" data-button-role="primary">Sign in</button></form><button id="auth-toggle" data-testid="auth-toggle" class="secondary">Create account</button><p id="err" role="alert"></p></div>';auth_toggle.onclick=()=>{reg=!reg;auth_title.textContent=reg?'Create account':'Sign in';auth_submit.textContent=reg?'Sign up':'Sign in';auth_toggle.textContent=reg?'Use existing account':'Create account'};document.querySelector('form').onsubmit=async e=>{e.preventDefault();let f=Object.fromEntries(new FormData(e.target));let endpoint=reg?'/auth/register':'/auth/login';let[r,x]=await api(endpoint,{method:'POST',body:f});if(!r.ok){err.textContent=x.error||'Unable to continue';return}token=x.token;localStorage.setItem('huddle.token',token);window.__huddle_token=token;load()}}
async function load(){let [r,x]=await api('/workspaces');if(!r.ok)return auth();if(!x.workspaces.length)return create();me=(await api('/auth/me'))[1].user;let w=x.workspaces[0];let d=(await api('/workspaces/'+w.slug))[1];cur=d.channels[0];render(w,d)}
function create(){root.innerHTML='<form data-testid="workspace-create-form" style="padding:40px"><h1>Create workspace</h1><input name="slug" value="team"><input name="name" value="My Team"><button class="primary" data-testid="workspace-general-submit">Create</button></form>';root.querySelector('form').onsubmit=async e=>{e.preventDefault();let f=new FormData(e.target);await api('/workspaces',{method:'POST',body:Object.fromEntries(f)});load()}}
async function render(w,d){root.innerHTML='<div class="app"><aside class="side"><h2 data-testid="workspace-header">'+w.name+'</h2><div data-testid="current-user">'+me.display_name+'</div><button class="danger" data-testid="logout-btn">Logout</button><h3>Channels</h3><div data-testid="channel-list">'+d.channels.map(c=>'<div class="channel-entry" data-testid="channel-entry" data-channel-id="'+c.id+'" data-channel-name="'+c.name+'"># '+c.name+'</div>').join('')+'</div><button class="secondary" data-testid="new-channel-btn">New channel</button></aside><main class="main"><h2 id="channel-title" data-testid="channel-title"># '+cur.name+'</h2><div id="message-list" data-testid="message-list"></div><form id="send"><input id="message-input" data-testid="message-input" name="body"><button class="primary" data-testid="send-btn">Send</button></form></main></div>';document.querySelectorAll('.channel-entry').forEach(e=>e.onclick=async()=>{cur=d.channels.find(c=>c.id==e.dataset.channelId);render(w,d)});
document.querySelector('[data-testid="new-channel-btn"]').onclick=()=>{let box=document.createElement('div');box.id='create-channel-modal';box.innerHTML='<form id="channel-create-form" data-testid="create-channel-form"><h3>Create channel</h3><input name="name" required placeholder="channel-name"><label><input type="checkbox" name="is_private" data-testid="create-channel-private"> Private</label><button class="primary" data-testid="create-channel-submit" data-button-role="primary">Create</button><button type="button" class="secondary" data-testid="create-channel-cancel">Cancel</button></form>';document.body.appendChild(box);box.querySelector('form').onsubmit=async e=>{e.preventDefault();let f=new FormData(e.target);let[r,x]=await api('/workspaces/'+w.slug+'/channels',{method:'POST',body:{name:f.get('name'),is_private:f.get('is_private')==='on'}});if(!r.ok){box.querySelector('h3').textContent=x.error||'Unable to create channel';return}box.remove();d.channels.push(x.channel);cur=x.channel;render(w,d)};box.querySelector('[data-testid="create-channel-cancel"]').onclick=()=>box.remove()};
logout.onclick=()=>{localStorage.removeItem('huddle.token');location.reload()};send.onsubmit=async e=>{e.preventDefault();let v=message-input.value;if(!v.trim())return;await api('/channels/'+cur.id+'/messages',{method:'POST',body:{body:v}});message-input.value='';show()};show()}
async function show(){let x=(await api('/channels/'+cur.id+'/messages'))[1];document.querySelector('#message-list').innerHTML=x.messages.reverse().map(m=>'<div class="row" data-testid="message" data-message-id="'+m.id+'"><b>'+m.author.display_name+'</b> <span class="muted">'+m.created_at+'</span><div data-testid="message-body">'+m.body+(m.edited_at?' (edited)':'')+'</div><button data-testid="open-thread-btn" data-id="'+m.id+'">'+(m.reply_count||0)+' replies</button> <button data-testid="reaction-button" data-id="'+m.id+'">😊</button> <button data-testid="edit-message" data-id="'+m.id+'">Edit</button> <button data-testid="delete-message" data-id="'+m.id+'">Delete</button></div>').join('')||'<p class="muted">No messages yet. Start the conversation.</p>';
document.querySelectorAll('[data-testid="delete-message"]').forEach(b=>b.onclick=async()=>{await api('/messages/'+b.dataset.id,{method:'DELETE'});show()});
document.querySelectorAll('[data-testid="edit-message"]').forEach(b=>b.onclick=async()=>{let v=prompt('Edit message');if(v!==null&&v.trim()){await api('/messages/'+b.dataset.id,{method:'PATCH',body:{body:v}});show()}});
document.querySelectorAll('[data-testid="reaction-button"]').forEach(b=>b.onclick=()=>{let old=b.parentNode.querySelector('.picker');if(old){old.remove();return}let p=document.createElement('div');p.className='picker';p.setAttribute('data-testid','emoji-picker');['😀','😊','👍','❤️','🎉','🚀'].forEach(e=>{let x=document.createElement('button');x.textContent=e;x.setAttribute('data-testid','emoji-option');x.onclick=async()=>{await api('/messages/'+b.dataset.id+'/reactions',{method:'POST',body:{emoji:e}});p.remove();show()};p.appendChild(x)});b.parentNode.appendChild(p)});
document.querySelectorAll('[data-testid="open-thread-btn"]').forEach(b=>b.onclick=async()=>{let panel=document.createElement('div');panel.id='thread-panel';panel.dataset.testid='thread-panel';panel.innerHTML='<h3>Thread</h3><div id="thread-list"></div><input data-testid="thread-input"><button class="primary" data-testid="thread-send">Reply</button><button class="secondary" data-testid="close-thread">Close</button>';document.body.appendChild(panel);let rr=(await api('/messages/'+b.dataset.id+'/replies'))[1];thread_list.innerHTML=(rr.replies||[]).map(q=>'<p>'+q.body+'</p>').join('');thread_send.onclick=async()=>{let v=thread_input.value;if(v.trim()){await api('/channels/'+cur.id+'/messages',{method:'POST',body:{body:v,parent_id:Number(b.dataset.id)}});thread_input.value='';show()}};close_thread.onclick=()=>panel.remove()})}
token?load():auth();
</script></body></html>'''
        b=x.encode();self.send_response(200);self.send_header('Content-Type','text/html');self.send_header('Content-Length',str(len(b)));self.end_headers();self.wfile.write(b)
def run(port,node):
    init(); s=ThreadingHTTPServer(('127.0.0.1',port),H);s.node=node;s.serve_forever()
if __name__=='__main__':
    import sys
    run(int(sys.argv[1]),int(sys.argv[2]))
