import BaseNode, { type HandleConfig, type NodeData } from './BaseNode';
import { Position } from "@xyflow/react";
import { FileJson } from 'lucide-react';

const handles: HandleConfig[] = [
  { type: 'target', position: Position.Left, id: 'in:topic' },
  { type: 'source', position: Position.Right, id: 'out:topic' },
];

export default function MapNode({selected, data}: {selected: boolean; data: NodeData}) {
  return (
    <BaseNode
    selected={selected}
      data={data}
      type="map"
      defaultLabel='Map Node'
      colorClass="purple"
      Icon={FileJson}
      handles={handles}
    />
  );
}