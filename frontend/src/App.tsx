import { ReactFlowProvider } from '@xyflow/react'
import './App.css'
import { Flow } from './components/flow'


function App() {

  return (
    <ReactFlowProvider>
  <Flow></Flow>

    </ReactFlowProvider>
  )
}

export default App
