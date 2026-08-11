<style>
  .spinner {
    display: inline-block;
    width: 10px;
    height: 10px;
    border-radius: 50%;
    border: 1.5px solid currentColor;
    border-top-color: transparent;
    opacity: 0.7;
    animation: spin 0.7s linear infinite;
  }
  @keyframes spin {
    to { transform: rotate(360deg); }
  }
</style>

<span class="spinner" aria-hidden="true"></span>
