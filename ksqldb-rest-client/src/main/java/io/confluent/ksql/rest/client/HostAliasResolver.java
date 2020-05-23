package io.confluent.ksql.rest.client;

/**
 * Interface for resolving aliases to a given host.  This is useful in testing, for example as it
 * allows domains to resolve to localhost, while still passing on the original hostname over http.
 */
public interface HostAliasResolver {

  /**
   * Resolves the given alias to another host.
   * @param alias The alias hostname.
   * @return The resolved hostname which will actually be contacted.
   */
  String resolve(String alias);
}
