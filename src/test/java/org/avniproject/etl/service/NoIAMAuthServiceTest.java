package org.avniproject.etl.service;

import org.avniproject.etl.config.AvniKeycloakConfig;
import org.avniproject.etl.config.CognitoConfig;
import org.avniproject.etl.domain.User;
import org.avniproject.etl.repository.UserRepository;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

/**
 * Running with no identity provider is only safe when none is configured. The guard exists so that
 * a server left on idpType none while Cognito or Keycloak is configured refuses to serve anyone
 * rather than serving everyone.
 * <p>
 * This used to load the whole application context and call the method with no assertion at all, so
 * it passed when the guard stayed quiet and failed when it fired. Which of those happened depended
 * on whatever Cognito and Keycloak settings the machine running it happened to have, which is why
 * it passed on CI and failed locally.
 */
class NoIAMAuthServiceTest {
    private final UserRepository userRepository = mock(UserRepository.class);
    private final CognitoConfig cognitoConfig = mock(CognitoConfig.class);
    private final AvniKeycloakConfig avniKeycloakConfig = mock(AvniKeycloakConfig.class);
    private final NoIAMAuthService noIAMAuthService = new NoIAMAuthService(userRepository, cognitoConfig, avniKeycloakConfig);

    @Test
    public void servesNobodyWhenCognitoIsConfigured() {
        when(cognitoConfig.isConfigured()).thenReturn(true);
        when(avniKeycloakConfig.isConfigured()).thenReturn(false);

        RuntimeException thrown = assertThrows(RuntimeException.class, () -> noIAMAuthService.getUserFromToken("someUserName"));

        assertEquals(true, thrown.getMessage().contains("idpType is set to none"));
        verify(userRepository, never()).findByUsername("someUserName");
    }

    @Test
    public void servesNobodyWhenKeycloakIsConfigured() {
        when(cognitoConfig.isConfigured()).thenReturn(false);
        when(avniKeycloakConfig.isConfigured()).thenReturn(true);

        assertThrows(RuntimeException.class, () -> noIAMAuthService.getUserFromToken("someUserName"));

        verify(userRepository, never()).findByUsername("someUserName");
    }

    @Test
    public void returnsTheUserWhenNeitherIsConfigured() {
        User expected = new User("someUserName", "user-uuid", 1L);
        when(cognitoConfig.isConfigured()).thenReturn(false);
        when(avniKeycloakConfig.isConfigured()).thenReturn(false);
        when(userRepository.findByUsername("someUserName")).thenReturn(expected);

        assertSame(expected, noIAMAuthService.getUserFromToken("someUserName"));
    }
}
