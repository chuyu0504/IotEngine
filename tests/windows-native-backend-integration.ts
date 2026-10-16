import assert from 'node:assert/strict';
import {randomBytes,randomUUID,pbkdf2Sync} from 'node:crypto';
import {resolve} from 'node:path';
import {apiBase,databaseUrl} from './architecture-fixture';
const probe=process.argv[2];
assert(probe,'usage: windows-native-backend-integration.ts <vpn_platform_http_probe.exe>');
const db=new Bun.SQL(databaseUrl);
const edge=randomUUID(),user=randomUUID(),role=randomUUID(),route=randomUUID();
const platform='00000000-0000-7000-8000-000000000001',admin='00000000-0000-7000-8000-000000000002',network='00000000-0000-7000-8000-000000000004';
const username='native-'+randomBytes(4).toString('hex'),password='DisposableNativeFixture123!';
const publicKey=()=>randomBytes(32).toString('base64');
try {
 const salt=randomBytes(16),hash=`pbkdf2_sha256$210000$${salt.toString('hex')}$${pbkdf2Sync(password,salt,210000,32,'sha256').toString('hex')}`;
 await db`INSERT INTO sys_role(id,code,name,status,permissions) VALUES(${role},${username},'Native fixture','enabled','["iot:vpn:query","iot:vpn:enroll","iot:edge:query"]'::jsonb)`;
 await db`INSERT INTO sys_user(id,username,password_hash,nickname,status) VALUES(${user},${username},${hash},'Native fixture','enabled')`;
 await db`INSERT INTO sys_user_role(id,user_id,role_id) VALUES(${randomUUID()},${user},${role})`;
 await db`UPDATE vpn_network SET hub_public_key=${publicKey()},hub_endpoint='127.0.0.1',hub_listen_port=51820,status='enabled' WHERE id=${network}`;
 await db`INSERT INTO edge_node(id,platform_id,imei,name,model,enrollment_status,approved_by,approved_at) VALUES(${edge},${platform},'900000000000001','Native fixture','test','approved',${admin},NOW())`;
 await db`INSERT INTO vpn_peer(id,network_id,peer_type,edge_node_id,name,public_key,assigned_ipv4,status) VALUES(${edge},${network},'edge',${edge},'Native fixture',${publicKey()},'100.96.0.20'::inet,'active')`;
 await db`INSERT INTO vpn_route(id,network_id,edge_peer_id,lan_interface,target_cidr,virtual_cidr,created_by,status) VALUES(${route},${network},${edge},'lan','192.168.1.0/24','172.31.10.0/24',${admin},'active')`;
 const child=Bun.spawn([resolve(probe),new URL(apiBase).port,'backend'],{stdin:'pipe',stdout:'pipe',stderr:'pipe'});
 child.stdin.write(JSON.stringify({username,password,edge,publicKey:publicKey()})+'\n');child.stdin.end();
 const timer=setTimeout(()=>child.kill(),45000);
 const [status,stdout,stderr]=await Promise.all([child.exited,new Response(child.stdout).text(),new Response(child.stderr).text()]);clearTimeout(timer);
 assert.equal(status,0,stderr||stdout||'native backend probe timed out');
 const peers=await db`SELECT status,revoked_at FROM vpn_peer WHERE user_id=${user} AND peer_type='windows'`;
 assert.equal(peers.length,1);assert.equal(peers[0].status,'revoked');assert(peers[0].revoked_at);
 console.log(stdout.trim());
} finally {await db.close();}
