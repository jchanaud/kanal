import { Split } from "lucide-react";
import type { BranchCondition, HandleConfig, NodeData } from "./BaseNode";
import BaseNode from "./BaseNode";
import { useState } from "react";
import { Position } from "@xyflow/react";

export default function BranchNode({ data, selected }: { data: NodeData; selected: boolean }) {
  const handles: HandleConfig[] = [
  { type: 'target', position: Position.Left, id: 'in:topic' },
  ];
  // Calculate node height based on number of conditions
  return (
    <BaseNode
      selected={selected}
      data={{ context: "input-topic", ...data }}
      type="branch"
      defaultLabel='Branch'
      colorClass="orange"
      Icon={Split}
      handles={handles.concat(data.conditions ? data.conditions.map((condition, index) => {
        index;
        return {
          type: 'source',
          position: Position.Right,
          id: `out-${condition.id}:topic`,
          label: condition.label,
        };
      }):[])}
    />
  );
}