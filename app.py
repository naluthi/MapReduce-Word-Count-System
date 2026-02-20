from flask import Flask, request, jsonify, send_from_directory

# --- Flask App ---
app = Flask(__name__, static_folder='.', static_url_path='')

@app.route('/')
def index():
    return send_from_directory('.', 'index.html')

@app.route('/api/wordcount', methods=['POST'])
def wordcount_api():
    if 'file' not in request.files:
        return "Missing file", 400
    file = request.files['file']
    try:
        num_threads = int(request.form.get('threads', '1'))
        if num_threads < 1:
            raise ValueError()
    except ValueError:
        return "Invalid thread count", 400
    # read lines
    content = file.stream.read().decode('utf-8').splitlines()
    single_counts, single_time = single_threaded_wordcount(content)
    multi_counts, multi_time = multi_threaded_wordcount(content, num_threads)
    return jsonify({
        'single_time': single_time,
        'multi_time': multi_time,
        'counts': multi_counts
    })

if __name__ == '__main__':
    app.run(debug=True)
