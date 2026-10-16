import assert from 'node:assert/strict';
import {join} from 'node:path';
import {apiBase,fixtureDirectory} from './architecture-fixture';
// The upload-startup fixture seeds these files before iot-engine starts.
const directory=join(fixtureDirectory,'firmware');
const end=Date.now()+15000;
while((await fetch(apiBase+'/internal/health/ready')).status!==200){if(Date.now()>end)throw Error('workers not ready');await Bun.sleep(50);}
assert.equal(await Bun.file(join(directory,'00000000-0000-4000-8000-000000000004.upload')).exists(),false);
assert.equal((await Bun.file(join(directory,'00000000-0000-4000-8000-000000000005.bin')).text()).trim(),'preserved');
assert.equal((await Bun.file(join(directory,'unrelated.upload')).text()).trim(),'unrelated');
console.log('PASS actual Service Worker startup reclaims abandoned upload and preserves registered and unrelated files');
