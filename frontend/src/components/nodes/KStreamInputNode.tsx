import BaseNode, { type HandleConfig, type NodeData } from './BaseNode';
import { Position } from "@xyflow/react";
import { Database } from 'lucide-react';

const handles: HandleConfig[] = [
  { type: 'source', position: Position.Right, id: 'out:topic' },
];

export default function KStreamInputNode({selected, data }: { selected: boolean; data: NodeData }) {
  return (
    <BaseNode
      selected={selected}
      data={{ context: "input-topic", ...data }}
      type="source"
      defaultLabel='Input Stream'
      colorClass="green"
      Icon={Database}
      handles={handles}
    />
  );
}