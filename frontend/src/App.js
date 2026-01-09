import React, { useState } from "react";
import "./App.css";

function App() {
  const [file, setFile] = useState(null);
  const [loading, setLoading] = useState(false);
  const [message, setMessage] = useState("");
  const [results, setResults] = useState([]);
  const [summary, setSummary] = useState({ intrinsic: 0, extrinsic: 0 });

  const handleUpload = async () => {
    if (!file) {
      alert("Please select a CSV file");
      return;
    }

    setLoading(true);
    setMessage("");
    setResults([]);
    setSummary({ intrinsic: 0, extrinsic: 0 });

    const formData = new FormData();
    formData.append("file", file);

    try {
      const response = await fetch("http://127.0.0.1:8000/api/predict", {
        method: "POST",
        body: formData,
      });

      const data = await response.json();

      if (!response.ok) {
        setMessage(data.detail?.message || "Upload failed");
      } else {
        setMessage(`✅ Success! ${data.rows_inserted} rows processed`);
        setResults(data.predictions || []);

        // Calculate summary
        const intrinsicCount = data.predictions.filter(
          (r) => r.predicted_type === "Intrinsic Bug"
        ).length;
        const extrinsicCount = data.predictions.filter(
          (r) => r.predicted_type === "Extrinsic Bug"
        ).length;

        setSummary({ intrinsic: intrinsicCount, extrinsic: extrinsicCount });
      }
    } catch (error) {
      setMessage("❌ Backend not running or network error");
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className="app">
      <div className="container">
        <h1>SAI – BUG Classifier</h1>

        <input
          type="file"
          accept=".csv,text/csv"
          onChange={(e) => setFile(e.target.files[0])}
        />

        <button onClick={handleUpload} disabled={loading || !file}>
          {loading ? "Processing..." : "Upload your bug report in CSV format"}
        </button>

        {message && <p className="message">{message}</p>}

        {/* Prediction Cards */}
        {results.length > 0 && (
          <div className="results">
            {results.map((r, i) => (
              <div
                key={i}
                className={`result-card bug ${
                  r.predicted_type === "Intrinsic Bug" ? "intrinsic" : "extrinsic"
                }`}
              >
                <h3>{r.title}</h3>
                <p>
                  <b>Predicted Type:</b> {r.predicted_type}
                </p>
                <p>
                  <b>Label:</b> {r.predicted_label}
                </p>
              </div>
            ))}
          </div>
        )}

        {/* Summary Table */}
        {results.length > 0 && (
          <div className="summary">
            <h2>Summary</h2>
            <table>
              <thead>
                <tr>
                  <th>Total Bugs</th>
                  <th>Intrinsic</th>
                  <th>Extrinsic</th>
                </tr>
              </thead>
              <tbody>
                <tr>
                  <td>{results.length}</td>
                  <td>{summary.intrinsic}</td>
                  <td>{summary.extrinsic}</td>
                </tr>
              </tbody>
            </table>
          </div>
        )}
      </div>
    </div>
  );
}

export default App;
