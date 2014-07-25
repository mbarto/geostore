package it.geosolutions.geostore.services.rest.security;

import java.io.IOException;
import java.net.MalformedURLException;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.HttpMethod;
import org.apache.commons.httpclient.HttpStatus;
import org.apache.commons.httpclient.methods.GetMethod;
import org.apache.log4j.Logger;
import org.springframework.security.web.authentication.preauth.AbstractPreAuthenticatedProcessingFilter;

/**
 * PreAuthenticationFilter that can call an external REST WebService to get authenticated principal.
 * 
 * @author Mauro Bartolomeoli
 *
 */
public class WebServicePreAuthenticationFilter extends
		AbstractPreAuthenticatedProcessingFilter {

	private final static Logger LOGGER = Logger.getLogger(WebServicePreAuthenticationFilter.class);
	
	private String webServiceUrlTemplate = null;
	private String headerProperty = null;
	private String searchUser = "^\\s*(.*)\\s*$";
	
	private Pattern searchUserRegex = Pattern.compile(searchUser);
    
    int connectTimeout = 5;
    int readTimeout = 10;
	
	
	public void setWebServiceUrlTemplate(String webServiceUrlTemplate) {
		this.webServiceUrlTemplate = webServiceUrlTemplate;
	}

	public void setHeaderProperty(String headerProperty) {
		this.headerProperty = headerProperty;
	}
	
	public void setSearchUser(String searchUser) {
        this.searchUser = searchUser;
        searchUserRegex = Pattern.compile(searchUser);
    }

	@Override
	protected Object getPreAuthenticatedCredentials(HttpServletRequest request) {
		return "";
	}

	@Override
	protected Object getPreAuthenticatedPrincipal(HttpServletRequest request) {
		if(webServiceUrlTemplate == null) {
			throw new IllegalArgumentException("WebServicePreAuthenticationFilter not properly configured, missing webServiceUrlTemplate");
		}
		if(headerProperty == null) {
			throw new IllegalArgumentException("WebServicePreAuthenticationFilter not properly configured, missing headerProperty");
		}
		
		String key = request.getHeader(headerProperty);
		if(key != null && !key.isEmpty()) {
			return callWebService(key);
		}
		return null;		
	}

	private String callWebService(String key) {
		String url = webServiceUrlTemplate.replace("{key}", key);
        HttpClient client = new HttpClient();
        client.getParams().setConnectionManagerTimeout(connectTimeout);
        client.getParams().setSoTimeout(readTimeout);
        HttpMethod method = new GetMethod(url);
        
        
        try {
        	int statusCode = client.executeMethod(method);
        	if (statusCode != HttpStatus.SC_OK) {
        		LOGGER.warn("Error in webservice call to " + url +" : " + statusCode);
        		return null;
            }

            // Read the response body.
            byte[] responseBody = method.getResponseBody();

              
            String result = new String(responseBody);
            
            if(searchUserRegex == null) {
                return result.toString();
            } else {
                Matcher matcher = searchUserRegex.matcher(result);
                if(matcher.find())  {
                    return matcher.group(1);
                }
            }
            
        } catch (MalformedURLException e) {
            LOGGER.error("Error in WebServicePreAuthenticationFilter, web service url is invalid: " + url, e);
        } catch (IOException e) {
            LOGGER.error("Error in WebServicePreAuthenticationFilter, error in web service communication", e);
        } finally {
            
        }
        return null;
	}

}
