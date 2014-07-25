package it.geosolutions.geostore.services.rest.security;

import it.geosolutions.geostore.core.model.User;
import it.geosolutions.geostore.services.UserService;

import java.util.ArrayList;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.authentication.BadCredentialsException;
import org.springframework.security.authentication.DisabledException;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.GrantedAuthorityImpl;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.preauth.PreAuthenticatedAuthenticationToken;

/**
 * AuthenticationProvider using PreAuthenticated filters.
 * 
 * @author Mauro Bartolomeoli
 * 
 */
public class PreAuthenticationProvider implements AuthenticationProvider {

    private final static Logger LOGGER = Logger.getLogger(PreAuthenticationProvider.class);
    
    @Autowired
    UserService userService;
    
    /**
     * Message shown if the user credentials are wrong. TODO: Localize it
     */
    private static final String UNAUTHORIZED_MSG = "Bad credentials";

    /**
     * Message shown if the user it's not found. TODO: Localize it
     */
    public static final String USER_NOT_FOUND_MSG = "User not found. Please check your credentials";
    public static final String USER_NOT_ENABLED = "The user present but not enabled";

    @Override
    public boolean supports(Class<? extends Object> authentication) {
        return authentication.equals(PreAuthenticatedAuthenticationToken.class);
    }        

    
	@Override
    public Authentication authenticate(Authentication authentication) {
				
		String us = (String) authentication.getPrincipal();
		if(us != null) {
	        User user = null;
	        try {
	            user = userService.get(us);
	            
	            if(!user.isEnabled()){
	            	throw new DisabledException(USER_NOT_FOUND_MSG);
	            }
	        } catch (Exception e) {
	            LOGGER.info(USER_NOT_FOUND_MSG);
	            user = null;
	        }
	
	        if (user != null) {
	            String role = user.getRole().toString();
	            // return null;
	            List<GrantedAuthority> authorities = new ArrayList<GrantedAuthority>();
	            authorities.add(new GrantedAuthorityImpl("ROLE_" + role));
	            Authentication a = new UsernamePasswordAuthenticationToken(user, "", authorities);
	            // a.setAuthenticated(true);
	            return a;
	        } else {
	            throw new UsernameNotFoundException(USER_NOT_FOUND_MSG);
	        }
		} else {
			throw new BadCredentialsException(UNAUTHORIZED_MSG);
		}

    }
}
