package ai.chronon.flink.test.window

import ai.chronon.flink.deser.ProjectedEvent
import ai.chronon.flink.window.{AlwaysFireOnElementTrigger, BufferedProcessingTimeTrigger}
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.streaming.api.windowing.triggers.{Trigger, TriggerResult}
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.mockito.ArgumentMatchers.any
import org.mockito.Mockito._
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers
import org.scalatestplus.mockito.MockitoSugar

class TriggerTest extends AnyFlatSpec with Matchers with MockitoSugar {

  "AlwaysFireOnElementTrigger" should "fire on every element" in {
    val trigger = new AlwaysFireOnElementTrigger()
    val mockCtx = mock[Trigger.TriggerContext]
    val window = new TimeWindow(0, 1000)
    val event = ProjectedEvent(Map("ts" -> 500L), System.currentTimeMillis())

    val result = trigger.onElement(event, 500L, window, mockCtx)

    result shouldBe TriggerResult.FIRE
  }

  it should "return CONTINUE on onProcessingTime" in {
    val trigger = new AlwaysFireOnElementTrigger()
    val mockCtx = mock[Trigger.TriggerContext]
    val window = new TimeWindow(0, 1000)

    val result = trigger.onProcessingTime(500L, window, mockCtx)

    result shouldBe TriggerResult.CONTINUE
  }

  it should "return CONTINUE on onEventTime when time is before window max timestamp" in {
    val trigger = new AlwaysFireOnElementTrigger()
    val mockCtx = mock[Trigger.TriggerContext]
    val window = new TimeWindow(0, 1000)

    val result = trigger.onEventTime(500L, window, mockCtx)

    result shouldBe TriggerResult.CONTINUE
  }

  it should "return FIRE on onEventTime when time equals window max timestamp" in {
    val trigger = new AlwaysFireOnElementTrigger()
    val mockCtx = mock[Trigger.TriggerContext]
    val window = new TimeWindow(0, 1000)

    val result = trigger.onEventTime(window.maxTimestamp(), window, mockCtx)

    result shouldBe TriggerResult.FIRE
  }

  it should "return FIRE on onEventTime when time exceeds window max timestamp" in {
    val trigger = new AlwaysFireOnElementTrigger()
    val mockCtx = mock[Trigger.TriggerContext]
    val window = new TimeWindow(0, 1000)

    val result = trigger.onEventTime(window.maxTimestamp() + 100, window, mockCtx)

    result shouldBe TriggerResult.FIRE
  }

  "BufferedProcessingTimeTrigger" should "return CONTINUE on element and register timer when no timer exists" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)
    val event = ProjectedEvent(Map("ts" -> 500L), System.currentTimeMillis())

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(null)
    when(mockCtx.getCurrentProcessingTime).thenReturn(100L)

    val result = trigger.onElement(event, 500L, window, mockCtx)

    result shouldBe TriggerResult.CONTINUE
    verify(mockCtx).registerProcessingTimeTimer(200L)
    verify(mockState).update(200L)
  }

  it should "return CONTINUE on element when timer already exists" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)
    val event = ProjectedEvent(Map("ts" -> 500L), System.currentTimeMillis())

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(java.lang.Long.valueOf(150L))

    val result = trigger.onElement(event, 500L, window, mockCtx)

    result shouldBe TriggerResult.CONTINUE
    verify(mockCtx, never()).registerProcessingTimeTimer(any[Long]())
    verify(mockState, never()).update(any[java.lang.Long]())
  }

  it should "return FIRE on onProcessingTime and clear state" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)

    val result = trigger.onProcessingTime(200L, window, mockCtx)

    result shouldBe TriggerResult.FIRE
    verify(mockState).update(null)
  }

  it should "return FIRE on onEventTime when pending timer state exists and time is before window max" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(java.lang.Long.valueOf(250L))

    val result = trigger.onEventTime(500L, window, mockCtx)

    result shouldBe TriggerResult.FIRE
  }

  it should "return CONTINUE on onEventTime when no pending state and time is before window max" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(null)

    val result = trigger.onEventTime(500L, window, mockCtx)

    result shouldBe TriggerResult.CONTINUE
  }

  it should "return FIRE on onEventTime at window max timestamp even without pending state" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(null)

    val result = trigger.onEventTime(window.maxTimestamp(), window, mockCtx)

    result shouldBe TriggerResult.FIRE
  }

  it should "return FIRE on onEventTime after window max timestamp even without pending state" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(null)

    val result = trigger.onEventTime(window.maxTimestamp() + 100, window, mockCtx)

    result shouldBe TriggerResult.FIRE
  }

  it should "fire at window close even with pending timer (cleanup delegated to clear())" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)
    val pendingTimerTs = java.lang.Long.valueOf(1050L)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(pendingTimerTs)

    val result = trigger.onEventTime(window.maxTimestamp(), window, mockCtx)

    // Should fire, but NOT cleanup here - Flink calls clear() immediately after
    result shouldBe TriggerResult.FIRE
    verify(mockCtx, never()).deleteProcessingTimeTimer(any[Long]())
    verify(mockState, never()).clear()
  }

  it should "properly clear state in clear() method" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)
    val pendingTimerTs = java.lang.Long.valueOf(1050L)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(pendingTimerTs)

    trigger.clear(window, mockCtx)

    verify(mockCtx).deleteProcessingTimeTimer(pendingTimerTs)
    verify(mockState).clear()
  }

  it should "handle clear() when no pending timer exists" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(null)

    trigger.clear(window, mockCtx)

    verify(mockCtx, never()).deleteProcessingTimeTimer(any[Long]())
    verify(mockState).clear()
  }

  // Tests for allowedLateness scenarios
  // When allowedLateness is configured, Flink's cleanup timer fires at window.maxTimestamp() + allowedLateness
  // Our trigger should still fire correctly in these cases

  "AlwaysFireOnElementTrigger with allowedLateness" should "fire when cleanup timer fires at maxTimestamp + allowedLateness" in {
    val trigger = new AlwaysFireOnElementTrigger()
    val mockCtx = mock[Trigger.TriggerContext]
    val window = new TimeWindow(0, 1000)
    val allowedLateness = 60000L // 1 minute

    // Flink calls onEventTime at window.maxTimestamp() + allowedLateness when allowedLateness is configured
    val cleanupTimerTime = window.maxTimestamp() + allowedLateness

    val result = trigger.onEventTime(cleanupTimerTime, window, mockCtx)

    result shouldBe TriggerResult.FIRE
  }

  "BufferedProcessingTimeTrigger with allowedLateness" should "fire when cleanup timer fires at maxTimestamp + allowedLateness without pending state" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)
    val allowedLateness = 60000L // 1 minute

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(null) // No pending timer - this was the bug scenario

    // Flink calls onEventTime at window.maxTimestamp() + allowedLateness when allowedLateness is configured
    val cleanupTimerTime = window.maxTimestamp() + allowedLateness

    val result = trigger.onEventTime(cleanupTimerTime, window, mockCtx)

    // Should still fire because cleanupTimerTime >= window.maxTimestamp()
    result shouldBe TriggerResult.FIRE
  }

  it should "fire at cleanup time with pending state (cleanup delegated to clear())" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)
    val allowedLateness = 60000L // 1 minute
    val pendingTimerTs = java.lang.Long.valueOf(1050L)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)
    when(mockState.value()).thenReturn(pendingTimerTs)

    val cleanupTimerTime = window.maxTimestamp() + allowedLateness

    val result = trigger.onEventTime(cleanupTimerTime, window, mockCtx)

    // Should fire, but NOT cleanup here - Flink calls clear() immediately after
    result shouldBe TriggerResult.FIRE
    verify(mockCtx, never()).deleteProcessingTimeTimer(any[Long]())
    verify(mockState, never()).clear()
  }

  // Test the specific bug scenario: processing-time timer fires, clears state, then window close
  "BufferedProcessingTimeTrigger bug scenario" should "fire final tile even after processing-time timer cleared state" in {
    val trigger = new BufferedProcessingTimeTrigger(100L)
    val mockCtx = mock[Trigger.TriggerContext]
    val mockState = mock[ValueState[java.lang.Long]]
    val window = new TimeWindow(0, 1000)

    when(mockCtx.getPartitionedState(any[ValueStateDescriptor[java.lang.Long]]())).thenReturn(mockState)

    // Step 1: Processing-time timer fires, clears state
    val processingTimeResult = trigger.onProcessingTime(200L, window, mockCtx)
    processingTimeResult shouldBe TriggerResult.FIRE
    verify(mockState).update(null)

    // Step 2: State is now null (simulating cleared state)
    when(mockState.value()).thenReturn(null)

    // Step 3: Cleanup timer fires at window.maxTimestamp()
    // This is where the bug was - original code would return CONTINUE here
    val eventTimeResult = trigger.onEventTime(window.maxTimestamp(), window, mockCtx)

    // With the fix, it should return FIRE to emit final complete tile
    eventTimeResult shouldBe TriggerResult.FIRE
  }
}
