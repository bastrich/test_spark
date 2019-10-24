package com.bastrich

import com.bastrich.utils.Task1BaseSpec

class Task1_2_Spec
  extends Task1BaseSpec {

  it("test enriching with sessions") {
    val task = new Task1_2
    testEnrichingWithSessions(task.enrichWithSessionIds)
  }

  it("test wrong input data schema") {
    val task = new Task1_2
    testWrongInputSchema(task.enrichWithSessionIds)
  }
}
