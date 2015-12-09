

<html>
	<head>
		<title>topics</title>
		<#include "/net/oneandone/reactive/kafka/rest/css.ftl">
	</head>
	
	<body> 
	     
	    <table>
            <tbody>

                <#assign odd = true>
                <#list topicnames as topicname>
                <#if odd>
                <tr class="odd">
                <#else>
                <tr class="even">
                </#if>
                <#assign odd = !odd>
                    <td> <a href="${self}topics/${topicname}">${topicname}</a>
                </tr>
                </tr>
                </#list>
            </tbody>
        </table> 
	
	    
	    <#include "/net/oneandone/reactive/kafka/rest/footer.ftl">
	</body>
</html>