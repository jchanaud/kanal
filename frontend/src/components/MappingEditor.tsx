import React from "react";
import type { NodeData } from "./nodes/BaseNode";
import { Button } from "./ui/button";
import { Edit, X, Map } from "lucide-react";
import editMapping from "@/assets/edit-mapping.png"; // Assuming you have an image in your assets folder

export default function MappingEditor ({
  isOpen,
  onClose,
  onSave,
  nodeData,
}: {
  isOpen: boolean
  onClose: () => void
  onSave: (data: any) => void
  nodeData: NodeData
}) {
  if (!isOpen) return null

  const handleSave = () => {
    // TODO: Implement save logic
    onSave({})
    onClose()
  }

  return (
    <div className="fixed inset-0 z-50 flex items-center justify-center bg-black/50">
      <div className="bg-white rounded-lg shadow-xl w-[95vw] h-[90vh] flex flex-col border-1">
        {/* Header */}
        <div className="flex items-center justify-between p-4 border-b border-gray-200">
          <h2 className="text-xl font-semibold">Edit Mapping - {nodeData.label || "Transform"}</h2>
          <div className="flex gap-2">
            <Button onClick={handleSave} className="bg-blue-600 hover:bg-blue-700">
              <Edit className="w-4 h-4 mr-2" />
              Save
            </Button>
            <Button variant="outline" onClick={onClose}>
              <X className="w-4 h-4 mr-2" />
              Close
            </Button>
          </div>
        </div>

        {/* Content Area - Empty for now */}
        <div className="flex-1 p-4">
          <div className="h-full flex items-center justify-center text-gray-500">
            <div className="text-center">
              {/* 
              <Map className="w-16 h-16 mx-auto mb-4 text-gray-400" />
              <p className="text-lg">Mapping Editor Content</p>
              <p className="text-sm">This is where the mapping configuration will go</p>
              */}
              {/* Load image from assets folder full size */}
              <img src={editMapping} alt="Mapping Editor Placeholder" className="w-full h-full object-contain" />
            </div>
          </div>
        </div>
      </div>
    </div>
  )
}