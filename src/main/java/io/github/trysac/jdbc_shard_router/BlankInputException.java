package io.github.trysac.jdbc_shard_router;

/** Thrown when a text input is empty or contains only whitespace. */
public final class BlankInputException extends IllegalArgumentException {

  private static final long serialVersionUID = 1L;

  /** Creates the exception with the standard message. */
  public BlankInputException() {
    super("input is blank");
  }
}
