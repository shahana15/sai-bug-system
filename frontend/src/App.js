import React, { useState } from "react";
import "./App.css";

function App() {
  const [file, setFile] = useState(null);
  const [results, setResults] = useState([]);
  const [loading, setLoading] = useState(false);

  const handleUpload = async () => {
    if (!file) {
      alert("Please select a CSV file first!");
      return;
    }
    setLoading(true);
    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await fetch("http://127.0.0.1:8000/api/predict", {
        method: "POST",
        body: formData,
      });

      if (!response.ok) {
        const err = await response.json();
        alert("Error: " + err.detail);
        setLoading(false);
        return;
      }

      const data = await response.json();
      setResults(data.results);
    } catch (error) {
      alert("Error uploading file. Make sure backend is running!");
      console.error(error);
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <div className="container">
        <h1>SAI - Intrinsic and Extrinsic BUG Prediction System</h1>
        <p className="subtitle">
          Upload your Bug report CSV and get predictions with explanations.
        </p>

        <input
          type="file"
          accept=".csv"
          onChange={(e) => setFile(e.target.files[0])}
        />
        <button onClick={handleUpload} disabled={loading}>
          {loading ? "Predicting..." : "Predict"}
        </button>

        {results && results.length > 0 && <hr />}

        <div className="results">
          {results.map((r, index) => (
            <div
              key={index}
              className={`result-card ${r.prediction === 1 ? "bug" : "clean"}`}
            >
              <h3>Row {r.row}</h3>
              <p>
                <b>Prediction:</b> {r.prediction === 1 ? "Bug-Inducing" : "Clean"}
              </p>
              <p>
                <b>Confidence:</b> {r.confidence}
              </p>
              <p>
                <b>Explanation:</b>
              </p>
              <ul>
                {r.explanation.map((e, i) => (
                  <li key={i}>{e}</li>
                ))}
              </ul>
            </div>
          ))}
        </div>
      </div>
    </div>
  );
}

export default App;
