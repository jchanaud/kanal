import BaseNode, { type HandleConfig, type NodeData } from './BaseNode';
import { Position } from "@xyflow/react";
import { Database } from 'lucide-react';

const handles: HandleConfig[] = [
  { type: 'target', position: Position.Left, id: 'in:topic' },
];

export default function TopicOutputNode({ selected, data }: { selected: boolean; data: NodeData }) {
  return (
    <BaseNode
    selected={selected}
      data={{ context: "output-topic", ...data }}
      type="sink"
      defaultLabel='Output Topic'
      colorClass="red"
      Icon={Database}
      handles={handles}
    />
  );
}