
import {getDatabase,ref,set} from "firebase/database"
import './App.css';
import {app} from './Firebase/Firebase';


const db=getDatabase(app);
function App() {
  const putData=()=>{
    set(ref(db,"users/laxmi"),{
      id:1,
      name:"laxmi yadav",
      age:19
    })
  };
  return (
    <div className="App">
      <h1>firebase React app</h1>
      <button onClick={putData}>put data</button>
    </div>
  );
}

export default App;
