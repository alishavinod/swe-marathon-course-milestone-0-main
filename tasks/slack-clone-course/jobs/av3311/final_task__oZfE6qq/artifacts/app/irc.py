import socket,threading,sqlite3,secrets,datetime
DB='/app/data/huddle.db'
def db():
 c=sqlite3.connect(DB);c.row_factory=sqlite3.Row;return c
def send(s,x): s.sendall((x+'\r\n').encode())
def main(csock,addr):
 nick=None; uid=None; chans=[]
 def prefix(): return (nick or '*')+'!user@localhost'
 try:
  f=csock.makefile('r',encoding='utf8',errors='ignore')
  for line in f:
   z=line.rstrip('\r\n'); parts=z.split(' ',2); cmd=parts[0].upper(); args=parts[1:]
   if cmd=='PASS':
    t=args[0] if args else ''; c=db();r=c.execute('select user_id from tokens where token=?',(t,)).fetchone();uid=r['user_id'] if r else None;c.close()
    if not uid:send(csock,':huddle 464 * :Password incorrect')
   elif cmd=='NICK':
    requested=args[0] if args else ''
    if nick and requested!=nick: send(csock,':huddle 433 * '+requested+' :Nickname in use')
    else:nick=requested
   elif cmd=='USER':
    if uid and nick:
     send(csock,':huddle 001 '+nick+' :Welcome to Huddle IRC')
     send(csock,':huddle 002 '+nick+' :Your host is huddle')
     send(csock,':huddle 003 '+nick+' :This server was created today')
     send(csock,':huddle 004 '+nick+' huddle huddle')
     send(csock,':huddle 005 '+nick+' CHANTYPES=# :are supported')
     send(csock,':huddle 422 '+nick+' :MOTD File is missing')
   elif cmd=='PING': send(csock,':huddle PONG '+(args[0] if args else 'huddle'))
   elif cmd=='PONG': pass
   elif cmd=='JOIN' and args:
    name=args[0]; 
    if not name.startswith('#') or '/' not in name:send(csock,':huddle 403 '+nick+' '+name+' :No such channel');continue
    slug,chname=name[1:].split('/',1);c=db(); ch=c.execute('select c.*,w.slug from channels c join workspaces w on w.id=c.workspace_id where w.slug=? and c.name=?',(slug,chname)).fetchone()
    if not ch:send(csock,':huddle 403 '+nick+' '+name+' :No such channel');c.close();continue
    chans.append((ch['id'],name));send(csock,':'+prefix()+' JOIN '+name)
    rows=c.execute('select u.username from users u join channel_members cm on cm.user_id=u.id where cm.channel_id=?',(ch['id'],)).fetchall(); names=' '.join(x['username'] for x in rows)
    send(csock,':huddle 353 '+nick+' = '+name+' :'+names);send(csock,':huddle 366 '+nick+' '+name+' :End of NAMES list');c.close()
   elif cmd=='NAMES' and args:
    name=args[0];send(csock,':huddle 353 '+nick+' = '+name+' :'+(nick or ''));send(csock,':huddle 366 '+nick+' '+name+' :End of NAMES list')
   elif cmd=='PRIVMSG' and len(parts)>2:
    name=args[0];body=parts[2].lstrip(':')
    hit=next((x for x in chans if x[1]==name),None)
    if not hit:send(csock,':huddle 404 '+nick+' '+name+' :Cannot send to channel');continue
    c=db();r=c.execute('select * from users where id=?',(uid,)).fetchone()
    with c:
     seq=c.execute('select coalesce(event_id,0)+1 from seq where channel_id=?',(hit[0],)).fetchone()[0];c.execute('insert into seq(channel_id,event_id) values(?,?) on conflict(channel_id) do update set event_id=?',(hit[0],seq,seq));c.execute('insert into messages(channel_id,author_id,body,created_at,event_id) values(?,?,?,?,?)',(hit[0],uid,body,datetime.datetime.now(datetime.timezone.utc).isoformat(),seq));c.commit()
    send(csock,':'+prefix()+' PRIVMSG '+name+' :'+body);c.close()
   elif cmd=='QUIT': break
   else:
    send(csock,':huddle 421 '+(nick or '*')+' '+cmd+' :Unknown command')
 except Exception: pass
 try:csock.close()
 except:pass
def run():
 s=socket.socket();s.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1);s.bind(('0.0.0.0',6667));s.listen()
 while 1:
  c,a=s.accept();threading.Thread(target=main,args=(c,a),daemon=True).start()
if __name__=='__main__':run()
