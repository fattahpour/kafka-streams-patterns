package com.fattahpour.kstreamspatterns.cqrsprojections;

public final class ProjectionOutcome {
  private ProjectionEvent event;
  private CommandError error;

  public ProjectionOutcome() {}

  public ProjectionOutcome(ProjectionEvent event, CommandError error) {
    this.event = event;
    this.error = error;
  }

  public ProjectionEvent event() {
    return event;
  }

  public CommandError error() {
    return error;
  }

  public void setEvent(ProjectionEvent event) {
    this.event = event;
  }

  public void setError(CommandError error) {
    this.error = error;
  }
}
