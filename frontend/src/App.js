


import React, { useState } from "react";
import "./App.css";

function App() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");

  const handleUpload = async () => {
    if (!file) {
      alert("Please select a CSV file");
      return;
    }

    setLoading(true);
    setMessage("");

    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await fetch("http://127.0.0.1:8000/api/predict", {
        method: "POST",
        body: formData,
      });

      const data = await response.json();

      if (!response.ok) {
        setMessage(data.detail || "Upload failed");
        setLoading(false);
        return;
      }

      setMessage(`✅ Success! ${data.rows_inserted} rows processed`);

    } catch (error) {
      setMessage("❌ Backend not running or network error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <div className="container">
        <h1>SAI – Bug Feature Preprocessing</h1>

        <input
          type="file"
          accept=".csv,text/csv"
          onChange={(e) => setFile(e.target.files[0])}
        />

        <button onClick={handleUpload} disabled={loading || !file}>
          {loading ? "Processing..." : "Upload CSV"}
        </button>

        {message && <p className="message">{message}</p>}
      </div>
    </div>
  );
}

export default App;
