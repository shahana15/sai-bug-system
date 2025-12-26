import React, { useState } from "react";
import "./App.css";

function App() {
  const [file, setFile] = useState(null);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);

  const handleUpload = async () => {
    if (!file) {
      alert("Upload CSV file");
      return;
    }

    setLoading(true);
    const formData = new FormData();
    formData.append("file", file);

    try {
      const res = await fetch("http://127.0.0.1:8000/api/predict", {
        method: "POST",
        body: formData,
      });

      if (!res.ok) {
        const err = await res.json();
        alert("Error: " + err.detail);
        setLoading(false);
        return;
      }

      const data = await res.json();
      setResults(data.results);
    } catch {
      alert("Backend not running");
    }
    setLoading(false);
  };

  return (
    <div className="app">
      <div className="container">
        <h1>SAI Bug Prediction System</h1>

        <input
          type="file"
          accept=".csv"
          onChange={(e) => setFile(e.target.files[0])}
        />
        <button onClick={handleUpload}>
          {loading ? "Processing..." : "Predict"}
        </button>

        <div className="results">
          {results.map((r, i) => (
            <div key={i} className="result-card bug">
              <h3>Row {r.row}</h3>
              <p>
                <b>Status:</b> {r.prediction}
              </p>
              <p>
                <b>Confidence:</b> {r.confidence}
              </p>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default App;
