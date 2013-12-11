/*
 *  Copyright (C) 2007 - 2011 GeoSolutions S.A.S.
 *  http://www.geo-solutions.it
 * 
 *  GPLv3 + Classpath exception
 * 
 *  This program is free software: you can redistribute it and/or modify
 *  it under the terms of the GNU General Public License as published by
 *  the Free Software Foundation, either version 3 of the License, or
 *  (at your option) any later version.
 * 
 *  This program is distributed in the hope that it will be useful,
 *  but WITHOUT ANY WARRANTY; without even the implied warranty of
 *  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *  GNU General Public License for more details.
 * 
 *  You should have received a copy of the GNU General Public License
 *  along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package it.geosolutions.geostore.services.rest.utils;

import it.geosolutions.geostore.core.dao.util.PwEncoder;
import it.geosolutions.geostore.core.model.User;
import it.geosolutions.geostore.core.model.enums.Role;
import it.geosolutions.geostore.services.UserService;
import it.geosolutions.geostore.services.exception.NotFoundServiceEx;

import java.util.List;
import java.util.Map;

import org.apache.cxf.configuration.security.AuthorizationPolicy;
import org.apache.cxf.interceptor.Fault;
import org.apache.cxf.interceptor.security.AccessDeniedException;
import org.apache.cxf.message.Message;
import org.apache.cxf.phase.AbstractPhaseInterceptor;
import org.apache.cxf.phase.Phase;
import org.apache.cxf.security.SecurityContext;
import org.apache.log4j.Logger;

/**
 *
 * Class GeoStoreAuthenticationInterceptor. Starting point was JAASLoginInterceptor.
 * 
 * @author ETj (etj at geo-solutions.it)
 * @author Tobia di Pisa (tobia.dipisa at geo-solutions.it)
 */
public class GeoStoreAuthenticationInterceptor extends AbstractPhaseInterceptor<Message> {

    private static final Logger LOGGER = Logger.getLogger(GeoStoreAuthenticationInterceptor.class);

    private UserService userService;
    
    private boolean autoCreateUsers = false;
    private Role newUsersRole = Role.USER;
    private NewPasswordStrategy newUsersPassword = NewPasswordStrategy.NONE;
    private String newUsersPasswordHeader = "";
    
    /**
	 * @param userService the userService to set
	 */
	public void setUserService(UserService userService) {
		this.userService = userService;
	}

	public GeoStoreAuthenticationInterceptor() {
        super(Phase.UNMARSHAL);
    }
	
    public void setAutoCreateUsers(boolean autoCreateUsers) {
		this.autoCreateUsers = autoCreateUsers;
	}
    
    

	public void setNewUsersRole(Role newUsersRole) {
		this.newUsersRole = newUsersRole;
	}

	public void setNewUsersPassword(NewPasswordStrategy newUsersPassword) {
		this.newUsersPassword = newUsersPassword;
	}

	public void setNewUsersPasswordHeader(String newUsersPasswordHeader) {
		this.newUsersPasswordHeader = newUsersPasswordHeader;
	}

	/* (non-Javadoc)
     * @see org.apache.cxf.interceptor.Interceptor#handleMessage(org.apache.cxf.message.Message)
     */
    @Override
    public void handleMessage(Message message) throws Fault {
    	if(LOGGER.isInfoEnabled()){
            LOGGER.info("In handleMessage");
            LOGGER.info("Message --> " + message) ;
    	}
        
        String username = null;
        String password = null;

        User user = null;

        AuthorizationPolicy policy = (AuthorizationPolicy)message.get(AuthorizationPolicy.class);
        if (policy != null) {
            username = policy.getUserName();
            password = policy.getPassword();
            if(password == null)
                password = "";

            if(LOGGER.isInfoEnabled())
            	LOGGER.info("Requesting user: " + username);
            
            // //////////////////////////////////////////////////////////////////
            // read user from DB: If user and PW do not match, 
            // throw new AuthenticationException("Unauthorized");
            // ///////////////////////////////////////////////////////////////////

            String encodedPw = null;
            try {
				user =  userService.get(username);
			} catch (NotFoundServiceEx e) {
	            if(LOGGER.isInfoEnabled())
	            	LOGGER.info("Requested user not found: " + username);
	            
				if (autoCreateUsers) {
					if (LOGGER.isInfoEnabled()) {
						LOGGER.info("Creating now");
					}
					user = new User();
					user.setName(username);
					
					password = getNewUserPassword(message, username);					
					user.setNewPassword(password);					
					user.setRole(newUsersRole);
					try {
						user.setId(userService.insert(user));
					} catch (Exception e1) {
						throw new AccessDeniedException(
								"Not able to create new user");
					}
				} else {
					throw new AccessDeniedException("Not authorized");
				}            
			} catch (Exception e) {
	            	LOGGER.error("Exception while checking pw: " + username, e);
	            throw new AccessDeniedException("Authorization error");
			}
            encodedPw = PwEncoder.encode(password);
            if( ! encodedPw.equalsIgnoreCase(user.getPassword())) {
	            if(LOGGER.isInfoEnabled())
	            	LOGGER.info("Bad pw for user " + username);
                throw new AccessDeniedException("Not authorized");
            }

        } else {
        	if(LOGGER.isInfoEnabled())
        		LOGGER.info("No requesting user -- GUEST access");
        }

        GeoStoreSecurityContext securityContext = new GeoStoreSecurityContext();
        GeoStorePrincipal principal = user != null ?
                new GeoStorePrincipal(user) : GeoStorePrincipal.createGuest();
        securityContext.setPrincipal(principal);

        message.put(SecurityContext.class, securityContext);
    }

	private String getNewUserPassword(Message message, String username) {
		switch(newUsersPassword) {
			case NONE:
				return "";
			case USERNAME:
				return username;
			case FROMHEADER:
				Map<String, List<String>> headers = (Map<String, List<String>>)message.get(Message.PROTOCOL_HEADERS);
				if(headers.containsKey(newUsersPasswordHeader)) {
					return headers.get(newUsersPasswordHeader).get(0);
				}
				return "";
			default:
				return "";
		}
	}
    
}