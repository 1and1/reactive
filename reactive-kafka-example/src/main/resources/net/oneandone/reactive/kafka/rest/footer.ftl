
<p/><p/><p/>
<p style="font-size: 0.6em;">


   <#if _principal??>
     ${_principal}<br>
   </#if>
   ${.now?string("yyyy.MM.dd  HH:mm:ss,SSS")}
   
   <a href="${_requestURL}">${_requestURL}</a> at ${_requestURL.host} Port ${_requestURL.port} (real host: ${_localhost})
</p>

 