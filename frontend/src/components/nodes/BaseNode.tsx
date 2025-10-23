import { Card, CardContent } from '../ui/card';
import { Plus, type LucideIcon } from 'lucide-react';
import { Position, Handle, useNodeId, type IsValidConnection, type Node } from '@xyflow/react';
import { Badge } from '../ui/badge';

export interface BaseNodeProps {
  selected?: boolean;
  data: NodeData;
  type: string;
  defaultLabel: string;
  colorClass: ColorClass; // Tailwind class
  Icon?: LucideIcon;
  handles?: HandleConfig[];
  onNodeContextMenu?: (event: React.MouseEvent, node: NodeData) => void;
}
export interface NodeData {
  label?: string
  context?: string
  conditions?: BranchCondition[],
  handlePicked?: PickedHandle,
  onNodeContextMenu?: (event: React.MouseEvent, nodeId: string, handle: HandleConfig) => void
}
export interface PickedHandle {
  dataType: string;
  handleType: 'source' | 'target';
  handleId: string; // ID of the handle being connected
  nodeId: string; // ID of the node where the handle is located
}
export interface BranchCondition {
  id: string
  condition: string
  label: string
}
export interface HandleConfig {
  type: 'source' | 'target';
  position: Position;
  id: string;
  style?: React.CSSProperties;
  label?: string; // Optional label for the handle
}

type ColorClass = 'green' | 'red' | 'purple' | 'orange';

const colorVariants: Record<ColorClass, { card: string; badge: string; icon: string; selected: string, handle: string }> = {
    'green': {
        'card': "border-green-200 bg-green-50",
        'badge': "text-green-600 bg-green-100",
        'icon': "text-green-600",
        'selected': "ring-green-500",
        'handle': "!bg-green-500"
    },
    'red': {
        'card': "border-red-200 bg-red-50",
        'badge': "text-red-600 bg-red-100",
        'icon': "text-red-600",
        'selected': "ring-red-500",
        'handle': "!bg-red-500"
    },
    'purple': {
        'card': "border-purple-200 bg-purple-50",
        'badge': "text-purple-600 bg-purple-100",
        'icon': "text-purple-600",
        'selected': "ring-purple-500",
        'handle': "!bg-purple-500"
    },
    'orange': {
        'card': "border-orange-200 bg-orange-50",
        'badge': "text-orange-600 bg-orange-100",
        'icon': "text-orange-600",
        'selected': "ring-orange-500",
        'handle': "!bg-orange-500"
    }
};

const isValidConnection: IsValidConnection = (connection) => {
  const sourceHandle = connection.sourceHandle ?? null;
  const targetHandle = connection.targetHandle ?? null;
  const sourceType = sourceHandle ? sourceHandle.split(':')[1] : null;
  const targetType = targetHandle ? targetHandle.split(':')[1] : null;
  console.log(sourceType, targetType);
  return !!sourceType && !!targetType && sourceType === targetType && connection.source !== connection.target; // Prevent self-connections
};


export default function BaseNode({selected, data, type, defaultLabel, colorClass, Icon, handles = [] }: BaseNodeProps) {
  const label = data.label && data.label.trim() !== "" ? data.label : defaultLabel;
  const nodeHeight = Math.max(80, 60 + handles.length * 25);
  const targetHandles = handles.filter(handle => handle.type === 'target');
  const sourceHandles = handles.filter(handle => handle.type === 'source');
  const nodeId = useNodeId() ?? '';
  const renderSingleHandle = (handle: HandleConfig, topPosition: {style: React.CSSProperties}, data: NodeData, nodeId: string) => {
    // Determine if the handle is a valid target based on the currently picked handle
    // Must be: a different node, same dataType and opposite handleType
    const differentNode = data.handlePicked?.nodeId !== nodeId
    const sameDataType = data.handlePicked?.dataType === handle.id.split(':')[1];
    const oppositeHandleType = data.handlePicked?.handleType !== handle.type;
    const validTarget = differentNode && sameDataType && oppositeHandleType;
    const isMe = data.handlePicked?.nodeId === nodeId && data.handlePicked?.handleId === handle.id;
    return (
      <div key={handle.id} >
        <Handle id={handle.id} type={handle.type} position={handle.position} 
        style={{ width: '15px', height: '15px', ...topPosition.style, ...handle.style }}
        className={`${colorVariants[colorClass].handle} ${validTarget  ? `animate-pulse ring-2 ${colorVariants[colorClass].selected}` : isMe ? ` ring-2 ${colorVariants[colorClass].selected}` : ''}`}
        isValidConnection={isValidConnection}
      />
      {
        handle.label && 
        <>
        <div
          className="absolute left-42 flex items-center"
          
          style={{ top: topPosition.style.top, transform: 'translateY(-50%)' }}
          title={handle.label}>
            <span className='text-xs text-gray-600 bg-white px-1 rounded border max-w-40 truncate'>{handle.label}</span>
            {/*<button
            onClick={(event) => {
              data.onNodeContextMenu && data.onNodeContextMenu(event, nodeId, handle);
            }}
            className="ml-3 p-1 text-gray-500 hover:text-blue-600 hover:bg-gray-200 rounded-full transition-colors"
            style={{ top: topPosition.style.top, }}
            >
            <Plus size={32} strokeWidth={4} />
          </button>
          */}
          </div>
          
          </>
      }
      </div>
    )
  };
  return (
  <Card className={`min-w-[150px] ${colorVariants[colorClass].card} ${selected ? "ring-1 " + colorVariants[colorClass].selected : ''}`}
   style={{ minHeight: nodeHeight }}>
    <CardContent className="p-3">
      <div className="flex items-center gap-2 mb-2">
        {Icon && <Icon className={`w-5 h-5 ${colorVariants[colorClass].icon}`}></Icon>}
        <Badge style={{ textTransform: 'capitalize' }} className={`text-sm font-medium ${colorVariants[colorClass].badge}`}>{type}</Badge>
      </div>
       <div className="text-sm font-medium">{label}</div>
      <div className="text-xs text-gray-500 mt-1">{data.context || ""}</div>
      {
        // Render target handles
        targetHandles.map((handle, index) => {
          const topPosition = targetHandles.length > 1 ? {style: { top: (index+1)/(targetHandles.length+1)  * 100 + "%" }} : {style: { }};
          return renderSingleHandle(handle, topPosition, data, nodeId);
        })
      }
      {
        // Render source handles
        sourceHandles.map((handle, index) => {
          const topPosition = sourceHandles.length > 1 ? {style: { top: (index+1)/(sourceHandles.length+1)  * 100 + "%" }} : {style: { }};
          return renderSingleHandle(handle, topPosition, data, nodeId);
        })
      }
    </CardContent>
  </Card>
  );
}

