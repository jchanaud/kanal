import BaseNode, { type HandleConfig, type NodeData } from './BaseNode';
import { Position } from "@xyflow/react";
import { Table } from 'lucide-react';

const handles: HandleConfig[] = [
  { type: 'target', position: Position.Left, id: 'in:topic' },
  { type: 'source', position: Position.Right, id: 'out:table' },
];

export default function ToTableNode({selected, data}: {selected: boolean; data: NodeData}) {
  return (
    <BaseNode
    selected={selected}
      data={data}
      type="to_table"
      defaultLabel='Stream to Table Node'
      colorClass="purple"
      Icon={Table}
      handles={handles}
    />
  );
}