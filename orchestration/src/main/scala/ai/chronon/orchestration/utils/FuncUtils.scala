package ai.chronon.orchestration.utils

import io.temporal.workflow.Functions.Proc

object FuncUtils {

  def toTemporalProc(f: => Unit): Proc = {
    new Proc {
      override def apply(): Unit = f
    }
  }

}
