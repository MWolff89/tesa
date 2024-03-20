import ReactDOM from "react-dom/client";
import { v4 as uuidv4 } from "uuid";
import App from "./App.tsx";
import "./index.css";

// if (document.cookie.indexOf("user_id") === -1) {
  // document.cookie = `opengpts_user_id=${uuidv4()}`;
document.cookie = `opengpts_user_id=1f51d6ef-5336-4b8a-82e8-6d452d54a197`;
// }

ReactDOM.createRoot(document.getElementById("root")!).render(<App />);
