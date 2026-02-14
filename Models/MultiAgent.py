# =============================================================================
#  TOOL FEASIBILITY GATING ALGORITHM (TFG)
#  Product Signature: TFG
# ------------------------------------------------------------------------------
#  File: Models/MultiAgent.py
#  Purpose: Multi-agent wrappers (composite scheduling policy, telemetry hooks).
#  Author: Muhammet Ali Ozturk (extended by collaborators)
#  Generated: 2026-02-14
#  Environment: Python 3.9+
# =============================================================================

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Dict, Optional

from Core.Task import Mode, Task
from Models.Policy_Base import Scheduling_Policy, System_State


@dataclass
class MultiAgentCompositePolicy(Scheduling_Policy):
    """Dispatches per-task decisions to agent-specific policies.

    The simulator remains single-queue/single-server, but tasks are tagged with
    agent_id_i32. This composite policy allows heterogeneous strategies in the
    same run while reusing the existing Scheduling_Policy interface.
    """

    agent_policies_dict_obj: Dict[int, Scheduling_Policy] = field(default_factory=dict)

    def _Policy_For_Task(self, task_task: Task) -> Scheduling_Policy:
        agent_id = int(getattr(task_task, "agent_id_i32", 0))
        if agent_id not in self.agent_policies_dict_obj:
            raise KeyError(f"No policy registered for agent_id_i32={agent_id}")
        return self.agent_policies_dict_obj[agent_id]

    def Decide_Mode(self, task: Task, state_system_state: System_State) -> Mode:
        return self._Policy_For_Task(task).Decide_Mode(task, state_system_state)

    def Should_Switch_Mode(self, task: Task, state_system_state: System_State) -> bool:
        policy = self._Policy_For_Task(task)
        return bool(policy.Should_Switch_Mode(task, state_system_state))

    def Observe_Task_Outcome(self, task: Task) -> None:
        policy = self._Policy_For_Task(task)
        obs = getattr(policy, "Observe_Task_Outcome", None)
        if callable(obs):
            obs(task)

    # Marker hook used by Simulator to pre-decide modes at ARRIVAL.
    # For multi-agent experiments we often want this for all policies.
    def TFG_Policy_Identifier(self) -> None:
        return
