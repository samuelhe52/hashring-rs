import { chromium } from 'file:///C:/Users/30256/.cache/codex-runtimes/codex-primary-runtime/dependencies/node/node_modules/playwright/index.mjs';
import assert from 'node:assert/strict';
import { directory } from '../../dashboard/client/src/navigation.ts';
const root='work/dashboard-directory-20261111/';
const browser=await chromium.launch({executablePath:'C:/Program Files/Google/Chrome/Application/chrome.exe',headless:true});
const page=await browser.newPage({viewport:{width:1440,height:940}});
page.setDefaultTimeout(10000);
const errors=[];
page.on('pageerror',e=>errors.push(e.message));
let checking='start';
const choose=async id=>{
 checking=id;
 const parent=directory.flatMap(g=>g.items).find(p=>p.children?.some(c=>c.id===id));
 if(parent){
  const toggle=page.locator('.index-rail:visible .index-entry').filter({has:page.locator('[data-entry-id="'+parent.id+'"]')}).locator('.index-expand');
  if(await toggle.getAttribute('aria-expanded')==='false')await toggle.click();
 }
 await page.locator('.index-rail:visible [data-entry-id="'+id+'"]').click();
};
try{
 await page.goto('http://localhost:4173/?run=my-latency-20261028-013354');
 await page.locator('.hero-readings strong').first().waitFor();
 await page.emulateMedia({reducedMotion:'reduce'});
 const checked=[];
 for(const group of directory)for(const entry of group.items){
  await choose(entry.id);
  if(entry.panel)await page.locator('.detail-window[open]').waitFor();
  for(const child of entry.children??[]){
   await choose(child.id);
   if(child.anchor){
    await page.locator('#'+child.anchor).waitFor();
    await page.waitForFunction(id=>document.activeElement?.id===id,child.anchor);
   }else await page.waitForFunction(id=>!!document.querySelector('.index-rail [data-entry-id="'+id+'"][aria-current="page"]'),child.id);
   checked.push(child.id);
  }
  checked.push(entry.id);
 }
 await choose('nodes-cpu');
 assert.match(await page.locator('.window-content').innerText(),/CPU/);
 await choose('files-logs');
 assert.equal(await page.locator('.window-content').getByRole('button',{name:'进程日志',exact:true}).getAttribute('aria-pressed'),'true');
 await choose('config-build');
 assert.equal(await page.locator('.window-content').getByRole('button',{name:'构建与环境',exact:true}).getAttribute('aria-pressed'),'true');
 await choose('compare-curve');
 await page.getByRole('combobox',{name:'对比图类型'}).waitFor();
 assert.equal(await page.getByRole('combobox',{name:'对比图类型'}).inputValue(),'curve');
 await choose('case');
 await page.locator('.stage-comparison .data-chart svg').first().waitFor();
 await page.mouse.move(10,10);
 await page.screenshot({path:root+'case.png'});
 const pin=await page.evaluate(()=>{const el=document.querySelector('.playback-stage');const b=document.querySelector('.window-body');b.scrollTop=40;const y=el.getBoundingClientRect().top;b.scrollTop=180;return{before:y,after:el.getBoundingClientRect().top,offset:b.scrollTop}});
 assert.ok(pin.offset>100 && Math.abs(pin.before-pin.after)<2,JSON.stringify(pin));
 await page.locator('.window-body').evaluate(el=>el.scrollTop=0);
 await choose('replay-events');
 await page.locator('.window-content').getByRole('button',{name:'结束',exact:true}).click();
 const feed=await page.locator('.playback-event-scroll').evaluate(el=>({height:el.clientHeight,total:el.scrollHeight,first:el.querySelector('li')?.textContent,last:el.querySelector('li:last-child')?.textContent}));
 assert.ok(feed.total>feed.height);
 checking='tablet-animation';
 await page.setViewportSize({width:1024,height:800});
 await page.emulateMedia({reducedMotion:'no-preference'});
 await page.getByRole('button',{name:'打开完整目录',exact:true}).click();
 const m=page.locator('.index-dialog');
 await m.waitFor();await page.waitForTimeout(300);
 await page.screenshot({path:root+'drawer.png'});
 await page.getByRole('button',{name:'关闭目录',exact:true}).click();
 assert.ok(await m.evaluate(el=>el.open&&el.classList.contains('closing')));
 await m.waitFor({state:'hidden'});
 assert.ok(await page.locator('.detail-window').evaluate(el=>el.open));
 checking='mobile';
 await page.setViewportSize({width:390,height:844});
 await page.getByRole('button',{name:'打开完整目录',exact:true}).click();
 await m.waitFor();await page.waitForTimeout(280);
 await page.screenshot({path:root+'sheet.png'});
 const box=await m.boundingBox();assert.ok(box.x>=0&&box.x+box.width<=390&&box.y>=0);
 await m.locator('[data-entry-id="files"]').click();
 await page.waitForFunction(()=>document.querySelector('#window-title')?.textContent==='文件与日志');
 assert.ok(!(await m.evaluate(el=>el.open)));
 checking='keyboard';
 await page.getByRole('button',{name:'打开完整目录',exact:true}).focus();
 await page.keyboard.press('Enter');await m.waitFor();
 for(let i=0;i<25;i++){await page.keyboard.press('Tab');assert.ok(await page.evaluate(()=>document.activeElement?.closest('dialog')?.classList.contains('index-dialog')))}
 await page.keyboard.press('Escape');await m.waitFor({state:'hidden'});
 assert.ok(await page.locator('.detail-window').evaluate(el=>el.open));
 await page.keyboard.press('Escape');await page.locator('.detail-window').waitFor({state:'hidden'});
 assert.equal(await page.locator('body').evaluate(el=>el.style.overflow),'');
 checking='mobile-nested';
 await page.getByRole('button',{name:'打开完整目录',exact:true}).click();
 await page.locator('.index-dialog [data-entry-id="observations"]').click();
 await page.getByRole('button',{name:'打开完整目录',exact:true}).click();
 await page.locator('.index-dialog [data-entry-id="observations-pressure"]').click();
 await page.locator('.index-dialog').waitFor({state:'hidden'});
 assert.match(await page.locator('.window-content').innerText(),/初始写入后的节点压力/);
 assert.ok(await page.evaluate(()=>document.documentElement.scrollWidth<=innerWidth));
 assert.deepEqual(errors,[]);
 console.log(JSON.stringify({entries:checked.length,checked,feed,pin,viewportChecks:[1440,1024,390],errors},null,2));
}catch(error){console.log('Failed at',checking);console.log(await page.evaluate(()=>({active:document.activeElement?.outerHTML.slice(0,220),current:[...document.querySelectorAll('[aria-current="page"]')].map(x=>x.dataset.entryId),title:document.querySelector('#window-title')?.textContent,scroll:document.querySelector('.window-body')?.scrollTop})));throw error}
finally{await browser.close()}
